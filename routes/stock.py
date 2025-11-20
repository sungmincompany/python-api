# routes/stock.py

from flask import Blueprint, request, jsonify
from db import get_db_connection

stock_bp = Blueprint('stock', __name__, url_prefix='/api/stock')

@stock_bp.route('/list', methods=['GET'])
def get_stock_list():
    """
    stock_sum_v6 뷰를 조회하여 재고 현황을 반환합니다.
    - tab_gbn_cd 파라미터가 '01'이면 완제품 재고만 조회
    - tab_gbn_cd 파라미터가 '02'이면 부자재 재고만 조회
    - 파라미터가 없으면 전체 재고 조회
    """
    v_db = request.args.get("v_db")
    tab_gbn_cd = request.args.get("tab_gbn_cd") # '01', '02' 또는 값 없음

    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # ---------------------------------------------------------
        # SQL 작성
        # 1. A: stock_sum_v6 (재고 뷰)
        # 2. B: jepum_code (제품 기준정보 - 이름, 구분코드 등)
        # ---------------------------------------------------------
        sql = """
            SELECT 
                A.jepum_cd, 
                B.jepum_nm, 
                A.stock_tot
            FROM stock_sum_v6 A
            LEFT JOIN jepum_code B ON A.jepum_cd = B.jepum_cd
            WHERE 1=1
        """
        
        params = []

        # 3. 구분 코드(tab_gbn_cd) 필터링 추가
        # 값이 들어왔을 때만 조건을 붙여서, 값이 없으면 전체가 조회되도록 함
        if tab_gbn_cd:
            sql += " AND B.tab_gbn_cd = ?"
            params.append(tab_gbn_cd)

        # 4. 정렬 (제품명 순)
        sql += " ORDER BY B.jepum_nm"

        # 쿼리 실행
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "jepum_cd": row[0],
                "jepum_nm": row[1] if row[1] else row[0],
                "stock_tot": float(row[2])
            })

        return jsonify(data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500