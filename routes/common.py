# routes/segsan.py

from flask import Blueprint, request, jsonify
from db import get_db_connection  # db.py가 루트에 있다고 가정 (경로에 따라 ..utils 등으로 수정)
import datetime

common_bp = Blueprint('common', __name__, url_prefix='/api/common')

@common_bp.route('/jepum', methods=['GET'])
def get_jepum_list():
    v_db = request.args.get("v_db")
    # 1. tab_gbn_cd 파라미터를 받습니다 (없으면 None)
    tab_gbn_cd = request.args.get("tab_gbn_cd") 

    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # 2. 기본 쿼리 작성 (WHERE 1=1은 조건을 뒤에 편하게 붙이기 위한 꼼수입니다)
        sql = "SELECT jepum_cd, jepum_nm FROM jepum_code WHERE 1=1"
        params = []

        # 3. 파라미터가 있으면 조건 추가 (완제품만 조회)
        if tab_gbn_cd:
            sql += " AND tab_gbn_cd = ?"
            params.append(tab_gbn_cd)

        # 정렬 추가
        sql += " ORDER BY jepum_nm"

        # 쿼리 실행 (params 튜플로 변환)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "jepum_cd": row[0],
                "jepum_nm": row[1]
            })
            
        return jsonify(data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500