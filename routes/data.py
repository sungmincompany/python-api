from flask import Blueprint, request, jsonify
from db import get_db_connection

# Blueprint 정의
data_bp = Blueprint('data_bp', __name__, url_prefix='/api/data')

# 헬퍼 함수: row 데이터를 딕셔너리로 변환
def row_to_dict(row):
    """
    DB row를 프론트엔드 포맷으로 변환합니다.
    cr_dt의 밀리초까지 포함하여 고유성을 확보합니다.
    """
    if not row:
        return {}
        
    # ⭐️ [수정 1] 날짜 포맷 변경: 'YYYY-MM-DD HH:MM:SS.mmm' (밀리초 3자리 포함)
    # Python의 %f는 마이크로초(6자리)이므로, SQL Server DATETIME(3자리)에 맞춰 뒤 3자리를 자릅니다.
    cr_dt = row[0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if row[0] else None
    
    auto_id = str(row[1]) # auto_id는 문자열로 확실하게 변환
    col_1 = float(row[2]) if row[2] is not None else 0
    col_2 = float(row[3]) if row[3] is not None else 0
    
    # ⭐️ 키 생성: 이제 cr_dt에 밀리초가 포함되어 겹치지 않습니다.
    # 예: "2025-12-12 11:04:14.753_235"
    key = f"{cr_dt}_{auto_id}"

    return {
        "key": key,
        "dateTime": cr_dt, 
        "auto_id": auto_id,
        "data1": str(col_1),
        "data2": str(col_2),
        "col_1_db": col_1,
        "col_2_db": col_2,
    }

# ==============================================================
# 1. [GET] 측정 데이터 조회
# ==============================================================
@data_bp.route('/measurement', methods=['GET'])
def get_measurement_data():
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")
    to_dt   = request.args.get("to_dt")

    if not v_db: 
        return jsonify({"error": "v_db missing"}), 400

    try:
        conn = get_db_connection(v_db)
        if conn is None:
            return jsonify({"error": "DB 연결 실패"}), 500
            
        cur = conn.cursor()

        sql = """
            SELECT 
                cr_dt, auto_id, col_1, col_2, 
                ymd, hh, mm, ss 
            FROM smart_log5
            WHERE 
                cr_dt >= CONVERT(DATETIME, ?, 112) AND 
                cr_dt < DATEADD(DAY, 1, CONVERT(DATETIME, ?, 112))
            ORDER BY cr_dt DESC, auto_id
        """
        cur.execute(sql, (from_dt, to_dt)) 
        rows = cur.fetchall()
        conn.close()

        data = [row_to_dict(row) for row in rows]
        return jsonify(data), 200
        
    except Exception as e:
        print(f"Error in get_measurement_data: {str(e)}")
        return jsonify({"error": f"데이터 조회 오류: {str(e)}"}), 500

# ==============================================================
# 2. [PUT] 측정 데이터 수정 (저장)
# ==============================================================
@data_bp.route('/measurement/update', methods=['PUT'])
def update_measurement_data():
    v_db = request.args.get("v_db")
    data = request.get_json()
    
    key = data.get("key")
    data1 = data.get("data1")
    data2 = data.get("data2")
    
    if not key or data1 is None or data2 is None: 
        return jsonify({"error": "필수 데이터 누락"}), 400

    try:
        # key 분리 ("2025-12-12 11:04:14.753_235") -> date와 auto_id
        cr_dt_str, auto_id = key.rsplit('_', 1)
        
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()
        
        # ⭐️ [수정 2] SQL 포맷 변경: 120(초 단위) -> 121(밀리초 포함 ODBC 포맷)
        sql = """
            UPDATE smart_log5 
            SET col_1 = ?, col_2 = ? 
            WHERE cr_dt = CONVERT(DATETIME, ?, 121) AND auto_id = ?
        """
        cur.execute(sql, (float(data1), float(data2), cr_dt_str, auto_id))
        conn.commit()
        conn.close()
        
        if cur.rowcount == 0:
            return jsonify({"message": "수정 실패: 일치하는 행 없음"}), 404
            
        return jsonify({"message": "수정 성공"}), 200
        
    except Exception as e:
        print(f"Error in update_measurement_data: {str(e)}")
        return jsonify({"error": f"데이터 수정 오류: {str(e)}"}), 500

# ==============================================================
# 3. [DELETE] 측정 데이터 삭제
# ==============================================================
@data_bp.route('/measurement/delete', methods=['DELETE'])
def delete_measurement_data():
    v_db = request.args.get("v_db")
    key = request.args.get("key") 

    if not key: return jsonify({"error": "key 누락"}), 400

    try:
        cr_dt_str, auto_id = key.rsplit('_', 1)
        
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()
        
        # ⭐️ [수정 2] SQL 포맷 변경: 121 사용
        sql = """
            DELETE FROM smart_log5 
            WHERE cr_dt = CONVERT(DATETIME, ?, 121) AND auto_id = ?
        """
        cur.execute(sql, (cr_dt_str, auto_id))
        conn.commit()
        conn.close()
        
        if cur.rowcount == 0:
            return jsonify({"message": "삭제 실패: 일치하는 행 없음"}), 404
            
        return jsonify({"message": "삭제 성공"}), 200
        
    except Exception as e:
        print(f"Error in delete_measurement_data: {str(e)}")
        return jsonify({"error": f"데이터 삭제 오류: {str(e)}"}), 500


# ==============================================================
# 4. [POST] 측정 데이터 복제 (auto_id 유지, 시간만 현재로)
# ==============================================================
@data_bp.route('/measurement/duplicate', methods=['POST'])
def duplicate_measurement_data():
    v_db = request.args.get("v_db")
    data = request.get_json()
    
    # 복제할 데이터의 원본 키
    original_key = data.get("original_key")
    
    if not original_key: 
        return jsonify({"error": "original_key 누락"}), 400

    try:
        # 키 분리 (날짜와 ID 추출)
        original_cr_dt_str, original_auto_id = original_key.rsplit('_', 1)
        
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()

        # 1. 원본 데이터 조회 (밀리초 포함 포맷 121 사용)
        select_sql = """
            SELECT col_1, col_2, ymd, hh, mm, ss
            FROM smart_log5
            WHERE cr_dt = CONVERT(DATETIME, ?, 121) AND auto_id = ?
        """
        cur.execute(select_sql, (original_cr_dt_str, original_auto_id))
        row = cur.fetchone()

        if not row:
            conn.close()
            return jsonify({"message": "복제할 원본 데이터 없음"}), 404

        col_1, col_2, ymd, hh, mm, ss = row
        
        # ⭐️ [수정됨] 새로운 ID를 생성하지 않고, 원본 ID를 그대로 사용합니다.
        new_auto_id = original_auto_id
        
        # 2. 새로운 행 삽입 
        # (cr_dt 컬럼을 생략했으므로 DB 설정에 따라 현재 시간이 자동으로 들어갑니다)
        insert_sql = """
            INSERT INTO smart_log5 
            (auto_id, ymd, hh, mm, ss, col_1, col_2) 
            VALUES 
            (?, ?, ?, ?, ?, ?, ?)
        """
        cur.execute(insert_sql, (new_auto_id, ymd, hh, mm, ss, col_1, col_2))
        conn.commit()
        conn.close()
        
        # 클라이언트에 성공 메시지 전달 (ID는 변하지 않았음을 알림)
        return jsonify({"message": "복제 성공", "new_auto_id": new_auto_id}), 200

    except Exception as e:
        print(f"Error in duplicate_measurement_data: {str(e)}")
        return jsonify({"error": f"데이터 복제 오류: {str(e)}"}), 500