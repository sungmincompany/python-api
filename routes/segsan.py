from flask import Blueprint, request, jsonify
from db import get_db_connection
import datetime

segsan_bp = Blueprint('segsan', __name__, url_prefix='/api/segsan')

# ==============================================================
#  1. [POST] 생산실적 등록
# ==============================================================
@segsan_bp.route('/insert', methods=['POST'])
def insert_segsan():
    v_db = request.args.get("v_db")
    if not v_db: return jsonify({"error": "v_db missing"}), 400
    
    data = request.get_json()
    segsan_dt = data.get("segsan_dt")
    jepum_cd  = data.get("jepum_cd")
    amt       = data.get("amt", 0)

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # PK 자동생성
        yy = segsan_dt[2:4]
        mm = segsan_dt[4:6]
        prefix = f"B{yy}{mm}F"
        cur.execute(f"SELECT MAX(segsan_cd) FROM segsan_mst WHERE segsan_cd LIKE '{prefix}%'")
        row = cur.fetchone()
        new_seq = str(int(row[0][-5:]) + 1).zfill(5) if row and row[0] else "00001"
        new_segsan_cd = f"{prefix}{new_seq}"

        # INSERT
        sql = """
            INSERT INTO segsan_mst 
            (segsan_cd, segsan_seq, segsan_dt, jepum_cd, amt, 
             dept_cd, man_cd, write_nm, from_tm, to_tm, write_dt, stock_how, last_yn, segsan_plan_seq)
            VALUES 
            (?, '01', ?, ?, ?, 
             'P1200', '모바일', '모바일', '0900', '1800', GETDATE(), '1', 'Y', '01')
        """
        cur.execute(sql, (new_segsan_cd, segsan_dt, jepum_cd, amt))
        conn.commit()
        conn.close()
        return jsonify({"message": "등록 성공", "segsan_cd": new_segsan_cd}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==============================================================
#  2. [GET] 생산실적 조회 (기간별)
#  URL: /api/segsan/list?v_db=...&from_dt=20251101&to_dt=20251130
# ==============================================================
@segsan_bp.route('/list', methods=['GET'])
def get_segsan_list():
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt", "19000101")
    to_dt   = request.args.get("to_dt", "29991231")

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # 제품명(jepum_nm)을 가져오기 위해 JOIN 사용
        sql = """
            SELECT s.segsan_cd, s.segsan_dt, s.jepum_cd, j.jepum_nm, s.amt
            FROM segsan_mst s
            LEFT JOIN jepum_code j ON s.jepum_cd = j.jepum_cd
            WHERE s.segsan_dt BETWEEN ? AND ?
            ORDER BY s.segsan_dt DESC, s.segsan_cd DESC
        """
        cur.execute(sql, (from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "segsan_cd": row[0],
                "segsan_dt": row[1],
                "jepum_cd": row[2],
                "jepum_nm": row[3],
                "amt": float(row[4]) if row[4] else 0
            })
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==============================================================
#  3. [PUT] 생산실적 수정 (수량만 수정 가능하게)
# ==============================================================
@segsan_bp.route('/update', methods=['PUT'])
def update_segsan():
    v_db = request.args.get("v_db")
    data = request.get_json()
    segsan_cd = data.get("segsan_cd")
    amt = data.get("amt")

    if not segsan_cd: return jsonify({"error": "PK 누락"}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        cur.execute("UPDATE segsan_mst SET amt = ? WHERE segsan_cd = ?", (amt, segsan_cd))
        conn.commit()
        conn.close()
        return jsonify({"message": "수정 성공"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==============================================================
#  4. [DELETE] 생산실적 삭제
# ==============================================================
@segsan_bp.route('/delete', methods=['DELETE'])
def delete_segsan():
    v_db = request.args.get("v_db")
    segsan_cd = request.args.get("segsan_cd")

    if not segsan_cd: return jsonify({"error": "PK 누락"}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        cur.execute("DELETE FROM segsan_mst WHERE segsan_cd = ?", (segsan_cd,))
        conn.commit()
        conn.close()
        return jsonify({"message": "삭제 성공"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500