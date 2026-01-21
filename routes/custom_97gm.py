# routes/custom_97gm.py

from flask import Blueprint, request, jsonify
from db import get_db_connection

# 블루프린트 이름도 업체명으로 명확하게 지정
bp_97gm = Blueprint('custom_97gm', __name__)

# ==========================================
# [97gm 전용] View를 이용한 제품 목록 조회
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

        # DB에 생성해둔 View (jepum_code_line_v)를 조회
        # 조건 변경이 필요하면 파이썬 코드가 아닌 DB View만 수정하면 됨
        sql = "SELECT jepum_cd, jepum_nm FROM jepum_code_line_v WHERE jepum_cd between '40001' and '40090' ORDER BY jepum_cd"
        
        cur.execute(sql)
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
        print(f"Error fetching 97gm line view: {str(e)}")
        return jsonify({"error": str(e)}), 500