# routes/custom_97gm.py

from flask import Blueprint, request, jsonify
from db import get_db_connection

bp_97gm = Blueprint('custom_97gm', __name__)

# ==========================================
# [97gm 전용] 통합 제품 목록 조회
# URL: /api/97gm/jepum/line
# ==========================================
@bp_97gm.route('/jepum/line', methods=['GET'])
def get_jepum_line_v_list():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        
        # 완제품 등록/생산 등록 등에서 사용하는 공통 뷰 (v3)
        sql = "SELECT * FROM jepum_code_line_v3 ORDER BY jepum_cd"
        cur.execute(sql)
        
        column_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        data = [dict(zip(column_names, row)) for row in rows]
        
        # ✅ [추가] 리스트 API에서도 NULL 방지 처리
        for item in data:
            if item.get('sub_cnt') is None:
                item['sub_cnt'] = 0
            if item.get('stock_tot') is None:
                item['stock_tot'] = 0

        conn.close()
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ==========================================
# [97gm 전용] 재고 조회
# URL: /api/97gm/stock/list
# ==========================================
@bp_97gm.route('/stock/list', methods=['GET'])
def get_stock_list_custom():
    v_db = request.args.get("v_db")
    tab_gbn_cd = request.args.get("tab_gbn_cd") # 01:완제품, 02:부자재
    
    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        data = []

        # ==========================================================
        # CASE 1: 완제품 (01) -> v3 뷰 사용 (stock_tot_sub 활용)
        # ==========================================================
        if tab_gbn_cd == '01':
            sql = "SELECT * FROM jepum_code_line_v3 ORDER BY jepum_cd"
            cur.execute(sql)
            column_names = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            # 여기서 바로 가공합니다.
            for row in rows:
                item = dict(zip(column_names, row))
                
                # 안전한 0 처리
                sub_cnt = item.get('sub_cnt') or 0
                stock_tot = item.get('stock_tot') or 0
                stock_tot_sub = item.get('stock_tot_sub') or 0
                
                # ✅ [핵심] 하위 제품이 있으면(sub_cnt > 0), 합계(stock_tot_sub)를 메인 재고로 사용
                if sub_cnt > 0:
                    item['stock_tot'] = stock_tot_sub
                else:
                    item['stock_tot'] = stock_tot
                
                item['sub_cnt'] = sub_cnt # null 방지 처리된 값 넣기
                data.append(item)

        # ==========================================================
        # CASE 2: 부자재 (02) -> 직접 조인 (단순 리스트)
        # ==========================================================
        elif tab_gbn_cd == '02':
            sql = """
                SELECT 
                    A.jepum_cd, A.jepum_nm, A.spec, 
                    ISNULL(B.stock_tot, 0) as stock_tot,
                    0 as sub_cnt, 0 as stock_tot_sub, '0' as pum_gbn,
                    A.jepum_cd as root_jepum_cd
                FROM jepum_code A WITH (NOLOCK)
                LEFT OUTER JOIN stock_sum_v6 B WITH (NOLOCK) ON A.jepum_cd = B.jepum_cd
                WHERE A.tab_gbn_cd = '02'
                ORDER BY A.jepum_cd
            """
            cur.execute(sql)
            column_names = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            data = [dict(zip(column_names, row)) for row in rows]
        
        else:
            pass

        conn.close()
        return jsonify(data), 200

    except Exception as e:
        print(f"Error fetching 97gm stock custom: {str(e)}")
        return jsonify({"error": str(e)}), 500