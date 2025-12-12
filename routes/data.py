from flask import Blueprint, request, jsonify
from db import get_db_connection # 고객님의 db.py 파일에서 연결 함수를 가져옵니다.

# Blueprint 정의 (URL 접두사는 /api/data로 설정)
data_bp = Blueprint('data_bp', __name__, url_prefix='/api/data')

# 헬퍼 함수: row 데이터를 딕셔너리로 변환
def row_to_dict(row):
    """
    DB row를 프론트엔드에서 사용할 딕셔너리 형태로 변환합니다.
    smart_log5 테이블의 스키마에 맞춰 필드를 매핑합니다.
    """
    if not row:
        return {}
        
    # cr_dt, auto_id, col_1, col_2 순서라고 가정 (SELECT * FROM smart_log5 ...에서)
    # 실제 쿼리에 맞춰 인덱스를 조정하세요.
    cr_dt = row[0].strftime('%Y-%m-%d %H:%M:%S') if row[0] else None
    auto_id = row[1]
    col_1 = float(row[2]) if row[2] is not None else 0
    col_2 = float(row[3]) if row[3] is not None else 0
    
    # 여기서 key는 프론트엔드에서 사용하는 식별자입니다.
    # cr_dt와 auto_id를 조합하여 고유 키로 사용합니다.
    key = f"{cr_dt}_{auto_id}"

    return {
        "key": key,
        "dateTime": cr_dt, # 프론트엔드의 '일시'
        "auto_id": auto_id,
        "data1": str(col_1), # 프론트엔드의 'Data1'
        "data2": str(col_2), # 프론트엔드의 'Data2'
        "data3": "0", # smart_log5에 없으므로 임시 값
        "data4": "0", # smart_log5에 없으므로 임시 값
        "remarks": "", # 비고 필드에 대한 DB 필드가 없으므로 임시 값
        "col_1_db": col_1, # 실제 DB 필드 값 (수정 시 사용)
        "col_2_db": col_2,
    }

# ==============================================================
# 1. [GET] 측정 데이터 조회 (⭐️ 기간 필터링 적용)
# ==============================================================
@data_bp.route('/measurement', methods=['GET'])
def get_measurement_data():
    v_db = request.args.get("v_db")
    
    # ⭐️ 쿼리 파라미터에서 기간 정보 가져오기 (YYYYMMDD 형식 예상)
    from_dt = request.args.get("from_dt")
    to_dt   = request.args.get("to_dt")

    if not v_db: 
        return jsonify({"error": "v_db missing"}), 400

    try:
        conn = get_db_connection(v_db)
        if conn is None:
            return jsonify({"error": "DB 연결 실패"}), 500
            
        cur = conn.cursor()

        # ⭐️ 기간 필터링을 위한 SQL
        sql = """
            SELECT 
                cr_dt, auto_id, col_1, col_2, 
                ymd, hh, mm, ss 
            FROM smart_log5
            WHERE 
                -- 시작일 (YYYYMMDD 00:00:00 이상)
                cr_dt >= CONVERT(DATETIME, ?, 112) AND 
                -- 종료일 (종료일 다음 날의 00:00:00 미만)
                cr_dt < DATEADD(DAY, 1, CONVERT(DATETIME, ?, 112))
            ORDER BY cr_dt DESC, auto_id
        """
        # 파라미터는 from_dt, to_dt 순서로 SQL에 전달합니다.
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
    
    # key (cr_dt_auto_id), data1, data2를 사용합니다.
    key = data.get("key")
    data1 = data.get("data1") # 프론트엔드에서 수정한 값
    data2 = data.get("data2") # 프론트엔드에서 수정한 값
    
    if not key or data1 is None or data2 is None: 
        return jsonify({"error": "필수 데이터 누락 (key, data1, data2)"}), 400

    try:
        # key를 다시 cr_dt와 auto_id로 분리
        cr_dt_str, auto_id = key.rsplit('_', 1)
        
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()
        
        # NOTE: smart_log5 테이블에 PK가 없으므로, cr_dt와 auto_id를 모두 사용하여 WHERE 절을 구성합니다.
        sql = """
            UPDATE smart_log5 
            SET col_1 = ?, col_2 = ? 
            WHERE cr_dt = CONVERT(DATETIME, ?, 120) AND auto_id = ?
        """
        # data1, data2는 문자열로 넘어오므로 숫자로 변환합니다.
        # CONVERT(DATETIME, '2024-11-11 14:00:00', 120) 형식 사용
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
    # key를 쿼리 파라미터로 받습니다.
    key = request.args.get("key") 

    if not key: return jsonify({"error": "key 누락"}), 400

    try:
        cr_dt_str, auto_id = key.rsplit('_', 1)
        
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()
        
        sql = """
            DELETE FROM smart_log5 
            WHERE cr_dt = CONVERT(DATETIME, ?, 120) AND auto_id = ?
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
# 4. [POST] 측정 데이터 복제 (새로운 항목으로 삽입)
# ==============================================================
@data_bp.route('/measurement/duplicate', methods=['POST'])
def duplicate_measurement_data():
    v_db = request.args.get("v_db")
    data = request.get_json()
    
    # 복제할 데이터의 원본 키와 복제 후 사용할 새로운 auto_id를 받습니다.
    original_key = data.get("original_key")
    
    if not original_key: 
        return jsonify({"error": "original_key 누락"}), 400

    try:
        original_cr_dt_str, original_auto_id = original_key.rsplit('_', 1)
        
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()

        # 1. 원본 데이터 조회
        select_sql = """
            SELECT col_1, col_2, ymd, hh, mm, ss
            FROM smart_log5
            WHERE cr_dt = CONVERT(DATETIME, ?, 120) AND auto_id = ?
        """
        cur.execute(select_sql, (original_cr_dt_str, original_auto_id))
        row = cur.fetchone()

        if not row:
            conn.close()
            return jsonify({"message": "복제할 원본 데이터 없음"}), 404

        col_1, col_2, ymd, hh, mm, ss = row
        
        # 2. 새로운 auto_id (PK 대체) 생성 
        cur.execute("SELECT MAX(CAST(auto_id AS INT)) FROM smart_log5")
        max_id_row = cur.fetchone()
        
        # auto_id가 문자열 '001' 등의 형식일 수 있으므로, INT로 변환 후 문자열로 다시 변환합니다.
        new_auto_id = str(int(max_id_row[0]) + 1).zfill(len(original_auto_id)) if max_id_row and max_id_row[0] else "001"
        
        # 3. 새로운 행 삽입 (cr_dt는 현재 시각을 사용하도록 DB DEFAULT 값 사용)
        insert_sql = """
            INSERT INTO smart_log5 
            (auto_id, ymd, hh, mm, ss, col_1, col_2) 
            VALUES 
            (?, ?, ?, ?, ?, ?, ?)
        """
        # 여기서는 cr_dt가 DEFAULT로 현재 시각을 가지도록 하고, ymd~ss는 원본의 값을 사용합니다.
        cur.execute(insert_sql, (new_auto_id, ymd, hh, mm, ss, col_1, col_2))
        conn.commit()
        conn.close()
        
        # 클라이언트가 갱신된 데이터를 다시 불러오도록 성공 응답을 보냅니다.
        return jsonify({"message": "복제 성공", "new_auto_id": new_auto_id}), 200

    except Exception as e:
        print(f"Error in duplicate_measurement_data: {str(e)}")
        return jsonify({"error": f"데이터 복제 오류: {str(e)}"}), 500