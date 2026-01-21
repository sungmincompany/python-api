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
    # [수정 1] jepum_flg2 파라미터 받기
    jepum_flg2 = request.args.get("jepum_flg2")
    # [추가] 정렬 기준 파라미터 받기 (예: 'nm', 'cd')
    sort_type = request.args.get("sort_type")

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

        # [수정 2] jepum_flg2 조건 추가 (파라미터가 있을 때만 동작)
        if jepum_flg2:
            sql += " AND jepum_flg2 = ?"
            params.append(jepum_flg2)

        # [수정됨] 정렬 조건 단순화 (오름차순 전용)
        # sort_type이 'cd'면 코드순, 그 외엔 이름순
        if sort_type == 'cd':
            sql += " ORDER BY jepum_cd"
        else:
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
    
# ==========================================
# [수정됨] 거래처(Vender) 목록 조회
# 파라미터: v_db, tab_gbn_cd (01:매출처, 02:매입처 등)
# ==========================================
@common_bp.route('/vender', methods=['GET'])
def get_vender_list():
    v_db = request.args.get("v_db")
    tab_gbn_cd = request.args.get("tab_gbn_cd") # 파라미터 받기

    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # 1. 기본 쿼리 (vender_code 테이블)
        sql = "SELECT vender_cd, vender_nm FROM vender_code WHERE 1=1"
        params = []

        # 2. 구분 코드가 있으면 조건 추가
        if tab_gbn_cd:
            sql += " AND tab_gbn_cd = ?"
            params.append(tab_gbn_cd)

        sql += " ORDER BY vender_nm"
        
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            # 리액트에서 사용할 키 이름도 'vender_'로 통일
            data.append({
                "vender_cd": row[0],
                "vender_nm": row[1]
            })

        return jsonify(data), 200

    except Exception as e:
        print(f"Error fetching vender list: {str(e)}")
        return jsonify({"error": str(e)}), 500