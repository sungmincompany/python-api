from flask import Blueprint, request, jsonify
from db import get_db_connection
import json

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
    
# ==============================================================
# 5. [GET] 스마트로그 전압/전류 데이터 조회 (두영기전 분석용)
# ==============================================================
@data_bp.route('/smart-log', methods=['GET'])
def get_smart_log_data():
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt") 
    to_dt   = request.args.get("to_dt")   
    auto_id = request.args.get("auto_id") 
    only_valid = request.args.get("only_valid", "true") # ⭐️ 0값 제외 여부 파라미터 추가

    if not v_db: 
        return jsonify({"error": "v_db missing"}), 400

    try:
        conn = get_db_connection(v_db)
        if conn is None:
            return jsonify({"error": "DB 연결 실패"}), 500
            
        cur = conn.cursor()

        if auto_id:
            auto_id_cond = f"AND auto_id = '{auto_id}'" if auto_id in ['201', '203'] else ""
        else:
            auto_id_cond = "AND auto_id IN ('201', '203')"

        # ⭐️ 유효 데이터(0이 아닌 값) 조건 추가
        valid_cond = ""
        if only_valid.lower() == "true":
            valid_cond = "AND (ISNULL(col_3, 0) > 0 OR ISNULL(col_4, 0) > 0)"

        # 파이썬 f-string으로 동적 쿼리 생성
        sql = f"""
            SELECT ymd, hh, mm, auto_id, gbn, bigo, bigo2, col_3, col_4, cr_dt
            FROM dbo.smart_log
            WHERE cr_dt >= CONVERT(DATETIME, ?, 120) 
              AND cr_dt <= CONVERT(DATETIME, ?, 120)
              {auto_id_cond}
              {valid_cond}
            ORDER BY cr_dt DESC, auto_id ASC
        """
        
        cur.execute(sql, (from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "ymd": row[0],
                "hh": row[1],
                "mm": row[2],
                "auto_id": row[3],
                "gbn": row[4],
                "bigo": row[5],
                # bigo2는 더 이상 사용하지 않지만 호환성을 위해 유지
                "col_1": float(row[7]) if row[7] is not None else 0,
                "col_2": float(row[8]) if row[8] is not None else 0,
                "cr_dt": row[9].strftime('%Y-%m-%d %H:%M:%S') if row[9] else f"{row[0]} {row[1]}:{row[2]}"
            })
            
        return jsonify(data), 200
        
    except Exception as e:
        print(f"Error in get_smart_log_data: {str(e)}")
        return jsonify({"error": f"데이터 조회 오류: {str(e)}"}), 500
   
# ==============================================================
# 7. [POST] 분석 결과 보고서 저장
# ==============================================================
@data_bp.route('/analysis-history', methods=['POST'])
def save_analysis_history():
    data = request.get_json()
    
    v_db = data.get("v_db")
    analysis_type = data.get("analysis_type")
    auto_id = data.get("auto_id")
    target_col = data.get("target_col")
    search_start_dt = data.get("search_start_dt")
    search_end_dt = data.get("search_end_dt")
    summary_data = data.get("summary_data") # 딕셔너리를 받아옴

    if not v_db or not analysis_type:
        return jsonify({"error": "필수 데이터 누락"}), 400

    try:
        conn = get_db_connection(v_db)
        if conn is None:
            return jsonify({"error": "DB 연결 실패"}), 500
        
        cur = conn.cursor()
        sql = """
            INSERT INTO analysis_report_history 
            (analysis_type, auto_id, target_col, search_start_dt, search_end_dt, summary_data)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        # 파이썬 딕셔너리(객체)를 DB에 텍스트로 저장하기 위해 JSON 문자열로 변환
        summary_json = json.dumps(summary_data, ensure_ascii=False)
        
        cur.execute(sql, (analysis_type, auto_id, target_col, search_start_dt, search_end_dt, summary_json))
        conn.commit()
        conn.close()

        return jsonify({"message": "분석 보고서가 성공적으로 저장되었습니다."}), 200
        
    except Exception as e:
        print(f"Error in save_analysis_history: {str(e)}")
        return jsonify({"error": f"저장 오류: {str(e)}"}), 500

# ==============================================================
# 8. [GET] 분석 결과 보고서 목록 조회
# ==============================================================
@data_bp.route('/analysis-history', methods=['GET'])
def get_analysis_history():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db missing"}), 400

    try:
        conn = get_db_connection(v_db)
        if conn is None:
            return jsonify({"error": "DB 연결 실패"}), 500
            
        cur = conn.cursor()
        sql = """
            SELECT idx, analysis_type, auto_id, target_col, search_start_dt, search_end_dt, summary_data, cr_dt
            FROM analysis_report_history
            ORDER BY idx DESC
        """
        cur.execute(sql)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "idx": row[0],
                "analysis_type": row[1],
                "auto_id": row[2],
                "target_col": row[3],
                "search_start_dt": row[4],
                "search_end_dt": row[5],
                # DB에 저장된 JSON 문자열을 다시 파이썬 딕셔너리로 변환하여 프론트엔드에 전달
                "summary_data": json.loads(row[6]) if row[6] else {},
                "cr_dt": row[7].strftime('%Y-%m-%d %H:%M:%S') if row[7] else None
            })
            
        return jsonify(data), 200
        
    except Exception as e:
        print(f"Error in get_analysis_history: {str(e)}")
        return jsonify({"error": f"조회 오류: {str(e)}"}), 500

# ==============================================================
# 9. [GET] 용접공정 전용 실시간 관제 및 동적 임계치 데이터 조회
# ==============================================================
@data_bp.route('/welding-realtime', methods=['GET'])
def get_welding_realtime():
    v_db = request.args.get("v_db", "18_DY")

    try:
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()

        # 1. 실시간 데이터 (smart_last)
        sql_last = """
            SELECT auto_id, col_3, col_4, cr_dt
            FROM dbo.smart_last
            WHERE auto_id IN ('201', '203')
        """
        cur.execute(sql_last)
        last_rows = cur.fetchall()

        # 2. 동적 임계치 기준 (smart_log) - ⭐️ bigo2='A' 대신 정상 스펙 범위 내의 최근 5건 평균으로 변경!
        sql_baseline = """
            SELECT auto_id, AVG(col_3) as avg_v, AVG(col_4) as avg_a
            FROM (
                SELECT auto_id, col_3, col_4,
                       ROW_NUMBER() OVER(PARTITION BY auto_id ORDER BY cr_dt DESC) as rn
                FROM dbo.smart_log
                WHERE auto_id IN ('201', '203') 
                  AND col_3 BETWEEN 18 AND 22   -- 정상 전압 바운더리
                  AND col_4 BETWEEN 200 AND 240 -- 정상 전류 바운더리
            ) t
            WHERE rn <= 5
            GROUP BY auto_id
        """
        cur.execute(sql_baseline)
        baseline_rows = cur.fetchall()
        conn.close()

        # 결과 데이터 기본 구조 셋팅 (grade 항목은 제거)
        result = {
            "201": {"realtime": {"v":0, "a":0, "time":""}, "target": {"v": 20.0, "a": 220.0}},
            "203": {"realtime": {"v":0, "a":0, "time":""}, "target": {"v": 20.0, "a": 220.0}}
        }

        for r in last_rows:
            aid = str(r[0])
            if aid in result:
                result[aid]["realtime"] = {
                    "v": float(r[1]) if r[1] else 0.0,
                    "a": float(r[2]) if r[2] else 0.0,
                    "time": r[3].strftime('%H:%M:%S') if r[3] else ""
                }
        for r in baseline_rows:
            aid = str(r[0])
            if aid in result and r[1] and r[2]:
                result[aid]["target"] = {"v": round(float(r[1]), 1), "a": round(float(r[2]), 1)}

        return jsonify(result), 200
        
    except Exception as e:
        print(f"Error in get_welding_realtime: {str(e)}")
        return jsonify({"error": f"데이터 조회 오류: {str(e)}"}), 500

# ==============================================================
# 10. [GET] 절곡공정 전용 실시간 관제 데이터 조회 (smart_last 테이블)
# ==============================================================
@data_bp.route('/bending-realtime', methods=['GET'])
def get_bending_realtime():
    v_db = request.args.get("v_db", "18_DY")

    try:
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()

        # 절곡기(205)의 규격(col_1~5)과 횟수(col_6) 조회
        sql = """
            SELECT col_1, col_2, col_3, col_4, col_5, col_6, cr_dt
            FROM dbo.smart_last
            WHERE auto_id = '205'
        """
        cur.execute(sql)
        row = cur.fetchone()
        conn.close()

        if not row:
            return jsonify({"stroke": 0, "spec": 0, "time": ""}), 200

        col_1 = float(row[0]) if row[0] is not None else 0
        col_2 = float(row[1]) if row[1] is not None else 0
        col_3 = float(row[2]) if row[2] is not None else 0
        col_4 = float(row[3]) if row[3] is not None else 0
        col_5 = float(row[4]) if row[4] is not None else 0
        col_6 = float(row[5]) if row[5] is not None else 0
        cr_dt = row[6]

        # ⭐️ 토글 스위치 우선순위 로직 (1번이 최우선)
        spec_val = 0
        if col_1 == 1:
            spec_val = 1
        elif col_2 == 2:
            spec_val = 2
        elif col_3 == 3:
            spec_val = 3
        elif col_4 == 4:
            spec_val = 4
        elif col_5 == 5:
            spec_val = 5

        return jsonify({
            "stroke": int(col_6),
            "spec": spec_val,  # 0이면 대기중, 1~5면 해당 규격 작업중
            "time": cr_dt.strftime('%H:%M:%S') if cr_dt else ""
        }), 200
        
    except Exception as e:
        print(f"Error in get_bending_realtime: {str(e)}")
        return jsonify({"error": f"데이터 조회 오류: {str(e)}"}), 500

# ==============================================================
# 11. [GET] 절곡공정 시간대별 생산량 추이 (smart_log 테이블)
# ==============================================================
@data_bp.route('/bending-hourly', methods=['GET'])
def get_bending_hourly():
    v_db = request.args.get("v_db", "18_DY")

    try:
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()

        # ⭐️ [핵심] 오늘 날짜(ymd) 기준, 각 시간대(hh)별 누적타발(col_6)의 MAX - MIN 을 구하면
        # 해당 시간에 몇 번 절곡했는지 순수 생산량이 나옵니다.
        sql = """
            SELECT 
                hh, 
                (MAX(col_6) - MIN(col_6)) as hourly_production
            FROM dbo.smart_log
            WHERE auto_id = '205' 
              AND ymd = CONVERT(VARCHAR(8), GETDATE(), 112)
            GROUP BY hh
            ORDER BY hh ASC
        """
        cur.execute(sql)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            # 생산량이 0보다 작게 나오는 오류(누적값 리셋 등) 방지
            prod = float(row[1]) if row[1] is not None else 0
            if prod < 0: prod = 0 
            
            data.append({
                "time": f"{row[0]}시",
                "production": int(prod)
            })

        return jsonify(data), 200
        
    except Exception as e:
        print(f"Error in get_bending_hourly: {str(e)}")
        return jsonify({"error": f"데이터 조회 오류: {str(e)}"}), 500

# ==============================================================
# 12. [GET] 작업 진행 모니터링 데이터 조회 (segsan_req_mst)
# ==============================================================
@data_bp.route('/production-orders', methods=['GET'])
def get_production_orders():
    v_db = request.args.get("v_db", "18_DY") 

    try:
        conn = get_db_connection(v_db)
        if conn is None: 
            return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()

        # ⭐️ bigo10 ~ bigo14까지 모두 조회하도록 수정
        sql = """
            SELECT TOP 100 
                segsan_req_cd, segsan_req_dt, bigo4, bigo5, 
                bigo8, bigo10, bigo11, bigo12, bigo13, bigo14
            FROM dbo.segsan_req_mst
            ORDER BY segsan_req_dt DESC, segsan_req_cd DESC
        """
        cur.execute(sql)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "segsan_req_cd": row[0] if row[0] else "",
                "segsan_req_dt": row[1] if row[1] else "",
                "bigo4": row[2] if row[2] else "",  
                "bigo5": row[3] if row[3] else "",  
                "bigo8": row[4] if row[4] else "",  
                "bigo10": row[5] if row[5] else "", # 가공/조립
                "bigo11": row[6] if row[6] else "", # 용접
                "bigo12": row[7] if row[7] else "", # 절곡
                "bigo13": row[8] if row[8] else "", # 사급출고
                "bigo14": row[9] if row[9] else ""  # 사급입고
            })

        return jsonify(data), 200
        
    except Exception as e:
        print(f"Error in get_production_orders: {str(e)}")
        return jsonify({"error": f"데이터 조회 오류: {str(e)}"}), 500

# ==============================================================
# 13. [GET] 품질 대시보드 데이터 조회 (banpum_mst + smart_log 연동)
# ==============================================================
@data_bp.route('/quality-dashboard', methods=['GET'])
def get_quality_dashboard():
    v_db = request.args.get("v_db", "18_DY")
    from_dt = request.args.get("from_dt") # 'YYYY-MM-DD HH:MM:SS'
    to_dt   = request.args.get("to_dt")
    auto_id = request.args.get("auto_id")

    if not from_dt or not to_dt:
        return jsonify({"error": "날짜 파라미터 누락"}), 400

    # banpum_mst 조회를 위한 YYYYMMDD 포맷 변환
    from_ymd = from_dt[:10].replace("-", "")
    to_ymd = to_dt[:10].replace("-", "")

    try:
        conn = get_db_connection(v_db)
        if conn is None: return jsonify({"error": "DB 연결 실패"}), 500
        cur = conn.cursor()

        # 1. 수율 데이터 (banpum_mst) - 해당 기간의 용접불량(0201) 양품/불량 합계
        sql_yield = """
            SELECT ISNULL(SUM(ok_amt), 0) as total_ok, ISNULL(SUM(amt), 0) as total_err
            FROM dbo.banpum_mst
            WHERE banpum_dt >= ? AND banpum_dt <= ?
              AND err_cd = '0201'
        """
        cur.execute(sql_yield, (from_ymd, to_ymd))
        yield_row = cur.fetchone()
        total_ok = int(yield_row[0]) if yield_row else 0
        total_err = int(yield_row[1]) if yield_row else 0

        # 2. 센서 데이터 (smart_log) + 해당 일자의 불량 등록 여부 조인
        auto_id_cond = f"AND s.auto_id = '{auto_id}'" if auto_id and auto_id != 'all' else "AND s.auto_id IN ('201', '203')"

        sql_sensor = f"""
            SELECT 
                s.cr_dt, 
                s.col_3 as volt, 
                s.col_4 as ampere,
                ISNULL(b.amt, 0) as defect_cnt
            FROM dbo.smart_log s
            LEFT JOIN dbo.banpum_mst b 
                ON s.ymd = b.banpum_dt AND b.err_cd = '0201'
            WHERE s.cr_dt >= CONVERT(DATETIME, ?, 120) 
              AND s.cr_dt <= CONVERT(DATETIME, ?, 120)
              AND s.col_3 > 5
              {auto_id_cond}
            ORDER BY s.cr_dt ASC
        """
        cur.execute(sql_sensor, (from_dt, to_dt))
        sensor_rows = cur.fetchall()
        conn.close()

        sensor_data = []
        for r in sensor_rows:
            sensor_data.append({
                "time": r[0].strftime('%Y-%m-%d %H:%M:%S') if r[0] else "",
                "v": float(r[1]) if r[1] else 0,
                "a": float(r[2]) if r[2] else 0,
                "is_defect_day": True if r[3] > 0 else False
            })

        return jsonify({
            "yield": {"ok": total_ok, "err": total_err},
            "sensor": sensor_data
        }), 200

    except Exception as e:
        print(f"Error in get_quality_dashboard: {str(e)}")
        return jsonify({"error": f"데이터 조회 오류: {str(e)}"}), 500