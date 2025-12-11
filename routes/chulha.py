from flask import Blueprint, request, jsonify
from db import get_db_connection
import datetime

# Blueprint 설정
chulha_bp = Blueprint('chulha', __name__, url_prefix='/api/chulha')

# ==============================================================
#  1. [POST] 출하실적 등록
# ==============================================================
@chulha_bp.route('/insert', methods=['POST'])
def insert_chulha():
    v_db = request.args.get("v_db")
    if not v_db: return jsonify({"error": "v_db missing"}), 400
    
    data = request.get_json()
    chulha_dt = data.get("chulha_dt")
    jepum_cd  = data.get("jepum_cd")
    vender_cd = data.get("vender_cd") # 추가됨
    amt       = data.get("amt", 0)
    bigo      = data.get("bigo", "")  # 추가됨

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # -------------------------------------------------------
        # PK 자동생성 로직 (생산: B..., 출하: S... 로 구분 가정)
        # 예: S2512F00001 (S + YYMM + F + 일련번호)
        # -------------------------------------------------------
        yy = chulha_dt[2:4]
        mm = chulha_dt[4:6]
        prefix = f"S{yy}{mm}F" 

        # chulha_mst_temp 테이블 조회
        cur.execute(f"SELECT MAX(chulha_cd) FROM chulha_mst_temp WHERE chulha_cd LIKE '{prefix}%'")
        row = cur.fetchone()
        
        # 일련번호 생성 (기존 번호가 있으면 +1, 없으면 00001)
        if row and row[0]:
            last_seq = int(row[0][-5:]) # 뒤 5자리 숫자 추출
            new_seq = str(last_seq + 1).zfill(5)
        else:
            new_seq = "00001"
            
        new_chulha_cd = f"{prefix}{new_seq}"

        # -------------------------------------------------------
        # INSERT 실행
        # -------------------------------------------------------
        sql = """
            INSERT INTO chulha_mst_temp 
            (chulha_cd, chulha_dt, jepum_cd, vender_cd, amt, bigo, write_dt)
            VALUES 
            (?, ?, ?, ?, ?, ?, GETDATE())
        """
        cur.execute(sql, (new_chulha_cd, chulha_dt, jepum_cd, vender_cd, amt, bigo))
        
        conn.commit()
        conn.close()
        return jsonify({"message": "등록 성공", "chulha_cd": new_chulha_cd}), 200
        
    except Exception as e:
        print(f"Insert Error: {e}") # 디버깅용 로그
        return jsonify({"error": str(e)}), 500


# ==============================================================
#  2. [GET] 출하실적 조회 (기간별)
#  URL: /api/chulha/list?v_db=...&from_dt=...&to_dt=...
# ==============================================================
@chulha_bp.route('/list', methods=['GET'])
def get_chulha_list():
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt", "19000101")
    to_dt   = request.args.get("to_dt", "29991231")

    if not v_db: return jsonify({"error": "v_db missing"}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # -------------------------------------------------------
        # 조회 쿼리 (JOIN: 제품명, 거래처명 가져오기)
        # A: 출하테이블, B: 제품테이블, C: 거래처테이블
        # -------------------------------------------------------
        sql = """
            SELECT 
                a.chulha_cd, 
                a.chulha_dt, 
                a.jepum_cd, 
                b.jepum_nm, 
                a.vender_cd,
                c.vender_nm,
                a.amt,
                a.bigo
            FROM chulha_mst_temp a
            LEFT JOIN jepum_code b ON a.jepum_cd = b.jepum_cd
            LEFT JOIN vender_code c ON a.vender_cd = c.vender_cd
            WHERE a.chulha_dt BETWEEN ? AND ?
            ORDER BY a.chulha_dt DESC, a.chulha_cd DESC
        """
        
        cur.execute(sql, (from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "chulha_cd": row[0],
                "chulha_dt": row[1],
                "jepum_cd": row[2],
                "jepum_nm": row[3], # 제품명
                "vender_cd": row[4],
                "vender_nm": row[5], # 거래처명
                "amt": float(row[6]) if row[6] else 0,
                "bigo": row[7] if row[7] else ""
            })
            
        return jsonify(data), 200
        
    except Exception as e:
        print(f"List Error: {e}")
        return jsonify({"error": str(e)}), 500


# ==============================================================
#  3. [PUT] 출하실적 수정 (수량, 비고 수정 가능)
# ==============================================================
@chulha_bp.route('/update', methods=['PUT'])
def update_chulha():
    v_db = request.args.get("v_db")
    data = request.get_json()
    
    chulha_cd = data.get("chulha_cd")
    amt = data.get("amt")
    bigo = data.get("bigo") # 비고도 수정 가능하도록 추가

    if not chulha_cd: return jsonify({"error": "PK 누락"}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        
        # 수량과 비고 업데이트
        sql = "UPDATE chulha_mst_temp SET amt = ?, bigo = ? WHERE chulha_cd = ?"
        cur.execute(sql, (amt, bigo, chulha_cd))
        
        conn.commit()
        conn.close()
        return jsonify({"message": "수정 성공"}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ==============================================================
#  4. [DELETE] 출하실적 삭제
# ==============================================================
@chulha_bp.route('/delete', methods=['DELETE'])
def delete_chulha():
    v_db = request.args.get("v_db")
    chulha_cd = request.args.get("chulha_cd")

    if not chulha_cd: return jsonify({"error": "PK 누락"}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        
        sql = "DELETE FROM chulha_mst_temp WHERE chulha_cd = ?"
        cur.execute(sql, (chulha_cd,))
        
        conn.commit()
        conn.close()
        return jsonify({"message": "삭제 성공"}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500