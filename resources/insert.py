# resources/insert.py
import os
from flask import Blueprint, jsonify, request
from db import get_db_connection, DecimalEncoder
from datetime import datetime
from werkzeug.utils import secure_filename

suju_insert_bp = Blueprint("suju_insert", __name__)
stock_insert_bp = Blueprint("stock_insert", __name__)
etc_insert_bp = Blueprint("etc_insert", __name__)
file_insert_bp = Blueprint("file_insert", __name__)

# --- 신규 추가: 점검 리포트 저장을 위한 블루프린트 ---
report_insert_bp = Blueprint("report_insert", __name__)

# 업로드 폴더 설정
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@suju_insert_bp.route("/register", methods=["POST"])
def suju_insert():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "vendor parameter is required"}), 400

    try:
        data = request.get_json()
        suju_dt = data.get("suju_dt")    # 예: "2025-03-06"
        out_dt_to = data.get("out_dt_to")    # 예: "2025-03-06"
        jepum_cd = data.get("jepum_cd")
        vender_cd = data.get("vender_cd")
        amt = data.get("amt")
        bigo = data.get("bigo", "").encode('euc-kr')
        suju_seq = data.get("suju_seq", "01")  # default value '01'
        suju_gbn = data.get("suju_gbn", "02")  # default value '02'
        process_cd = data.get("process_cd", "01")  # default value '01'

        # 필수 필드 체크
        if not all([suju_dt, out_dt_to, jepum_cd, vender_cd, amt]):
            print(f"필수 필드 누락: {data}")
            return jsonify({"error": "필수 필드 누락"}), 400
        
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()

        # 1) 날짜 파싱 후 연도/월 추출
        dt = datetime.strptime(suju_dt, "%Y-%m-%d")
        yy = dt.strftime("%y")  # 예: "25"
        mm = dt.strftime("%m")  # 예: "03"

        # 2) prefix 생성: B + 연도2자리 + 월2자리 + "D"
        prefix = f"B{yy}{mm}D"

        # 3) 현재 prefix로 시작하는 suju_cd 중 최댓값을 조회하여 seq + 1
        check_sql = f"SELECT MAX(suju_cd) FROM suju_mst WHERE suju_cd LIKE '{prefix}%'"
        cur.execute(check_sql)
        row = cur.fetchone()

        if row and row[0]:
            # 예: B2503D00012
            last_code = row[0]
            # 뒤 5자리를 숫자로 변환
            last_seq = int(last_code[-5:])
            new_seq = last_seq + 1
        else:
            new_seq = 1

        # 4) 5자리로 zero-fill
        seq_str = str(new_seq).zfill(5)  # 예: "00013"

        # 5) 최종 suju_cd 조합
        suju_cd = prefix + seq_str

        # 6) 들어온 suju_dt, out_dt_to 값에서 '-' 제거 (예: "2025-03-06" -> "20250306")
        suju_dt = suju_dt.replace("-", "")
        out_dt_to = out_dt_to.replace("-", "")

        # 7) INSERT
        insert_sql = """
            INSERT INTO suju_mst (suju_cd, suju_seq, suju_gbn, suju_dt, out_dt_to, jepum_cd, vender_cd, amt, bigo, process_cd)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur.execute(insert_sql, (suju_cd, suju_seq, suju_gbn, suju_dt, out_dt_to, jepum_cd, vender_cd, amt, bigo, process_cd))
        conn.commit()
        conn.close()

        

        # 생성된 suju_cd를 응답으로 같이 내려주면 클라이언트에서 확인 가능
        return jsonify({"message": "주문 등록 성공", "suju_cd": suju_cd}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# 출고등록 API
@stock_insert_bp.route("/out", methods=["POST"])
def stock_insert_out():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "vendor parameter is required"}), 400

    try:
        data = request.get_json()
        inout_seq = data.get("inout_seq", "01")  # default value '01'
        inout_gbn = data.get("inout_gbn", "KZ")  # default value 'KZ'
        inout_dt = data.get("inout_dt")    # 예: "2025-03-06"
        jepum_cd = data.get("jepum_cd")
        confirm_amt = data.get("confirm_amt")
        process_fg = data.get("process_fg", "02")  # default value '02'
        rcv_nm = data.get("rcv_nm", "admin")  # default value 'admin'
        stock_cd_from = data.get("stock_cd_from", "01")  # default value '01'
        stock_cd_to = data.get("stock_cd_to", "01")
        write_nm = data.get("write_nm", "admin")  # default value 'admin'
        tm_1 = data.get("tm_1", "180000")  # default value '180000'
        vender_cd = data.get("vender_cd")
        bigo = data.get("bigo", "").encode('euc-kr')

        # 필수 필드 체크
        if not all([inout_dt, jepum_cd, confirm_amt, stock_cd_from, stock_cd_to]):
            print(f"필수 필드 누락: {data}")
            return jsonify({"error": "필수 필드 누락"}), 400
        
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()

        # 1) 날짜 파싱 후 연도/월 추출
        dt = datetime.strptime(inout_dt, "%Y-%m-%d")
        yy = dt.strftime("%y")  # 예: "25"
        mm = dt.strftime("%m")  # 예: "03"

        # 2) prefix 생성: B + 연도2자리 + 월2자리 + "I"
        prefix = f"B{yy}{mm}I"

        # 3) 현재 prefix로 시작하는 inout_no 중 최댓값을 조회하여 seq + 1
        check_sql = f"SELECT MAX(inout_no) FROM stock_mst WHERE inout_no LIKE '{prefix}%'"
        cur.execute(check_sql)
        row = cur.fetchone()

        if row and row[0]:
            # 예: B2503I00012
            last_code = row[0]
            # 뒤 5자리를 숫자로 변환
            last_seq = int(last_code[-5:])
            new_seq = last_seq + 1
        else:
            new_seq = 1

        # 4) 5자리로 zero-fill
        seq_str = str(new_seq).zfill(5)  # 예: "00013"

        # 5) 최종 inout_no 조합
        inout_no = prefix + seq_str

        # 6) 들어온 inout_dt 값에서 '-' 제거 (예: "2025-03-06" -> "20250306")
        inout_dt = inout_dt.replace("-", "")

        # 7) INSERT
        insert_sql = """
            INSERT INTO stock_mst (inout_no, inout_seq, inout_gbn, inout_dt, jepum_cd, confirm_amt, process_fg, rcv_nm, stock_cd_from, stock_cd_to, write_nm, tm_1, vender_cd, bigo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur.execute(insert_sql, (inout_no, inout_seq, inout_gbn, inout_dt, jepum_cd, confirm_amt, process_fg, rcv_nm, stock_cd_from, stock_cd_to, write_nm, tm_1, vender_cd, bigo))
        conn.commit()
        conn.close()

        return jsonify({"message": "등록 성공", "inout_no": inout_no}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 유니레즈 test 실적 등록 API
@etc_insert_bp.route("/test-result", methods=["POST"])
def insert_test_result():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON Body가 필요합니다."}), 400

    lot_no   = data.get("lot_no")
    jepum_cd = data.get("jepum_cd")
    amt      = data.get("amt")
    man_cd   = data.get("man_cd").encode('euc-kr')
    bin_no   = data.get("bin_no", "")
    work_dt  = data.get("work_dt")

    # --- 📌 [추가] React에서 보낸 lot_no2, dev_no 받기 ---
    # (값이 없을 경우 빈 문자열 ""을 기본값으로 사용)
    lot_no2  = data.get("lot_no2", "") 
    dev_no   = data.get("dev_no", "")
    # --- [추가] 끝 ---
    
    print(dev_no, "dev")

    if not all([lot_no, jepum_cd, amt, man_cd, work_dt]):
        return jsonify({"error": "필수 필드(lot_no, jepum_cd, amt, man_cd, work_dt) 누락"}), 400

    try:
        # "YYYY-MM-DD" → "YYYYMMDD"
        work_dt_str = work_dt.replace("-", "")

        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500

        cur = conn.cursor()
        
        # --- 📌 [수정] INSERT 쿼리에 lot_no2, dev_no 컬럼 추가 ---
        # (주의: DB의 실제 컬럼명과 일치해야 합니다)
        query = """
            INSERT INTO lot_hst (
                lot_no, jepum_cd, amt, man_cd, bigo_1, work_dt, prg_cd,
                lot_no2, dev_no, lot_seq
            )
            VALUES (?, ?, ?, ?, ?, ?, '170', ?, ?, 1)
        """
        
        # --- 📌 [수정] execute 파라미터에 lot_no2, dev_no 변수 추가 ---
        cur.execute(query, (
            lot_no, jepum_cd, amt, man_cd, bin_no, work_dt_str,
            lot_no2, dev_no
        ))
        conn.commit()
        conn.close()

        return jsonify({"message": "TEST 실적 등록 성공"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@etc_insert_bp.route("/tapping-result", methods=["POST"])
def insert_tapping():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db 필요"}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON Body가 필요합니다."}), 400

    lot_no       = data.get("lot_no")
    amt          = data.get("amt")
    reel_count   = data.get("reel_count")
    reel_min_amt = data.get("reel_min_amt")
    man_cd       = data.get("man_cd").encode('euc-kr')
    bin_no       = data.get("bin_no", "")
    jepum_cd     = data.get("jepum_cd", "")

    if not all([lot_no, amt, reel_count, reel_min_amt, man_cd]):
        return jsonify({"error": "필수 필드 누락"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500
        cur = conn.cursor()

        # 1) 170 여부
        check_170 = """
            SELECT COUNT(*) FROM lot_hst
            WHERE lot_no=? AND prg_cd='170'
        """
        cur.execute(check_170, (lot_no,))
        if cur.fetchone()[0] == 0:
            conn.close()
            return jsonify({"error": "TEST(170) 등록 이력이 없는 LOT"}), 400

        # 2) 180 중복
        check_180 = """
            SELECT COUNT(*) FROM lot_hst
            WHERE lot_no=? AND prg_cd='180' AND lot_seq=1
        """
        cur.execute(check_180, (lot_no,))
        if cur.fetchone()[0] > 0:
            conn.close()
            return jsonify({"error": "이미 Taping(180) 등록된 LOT"}), 400

        # 3) reel_count 만큼 INSERT
        leftover = amt
        for i in range(1, reel_count+1):
            if leftover > reel_min_amt:
                use_qty = reel_min_amt
            else:
                use_qty = leftover

            if i == reel_count:
                leftover_after = leftover - use_qty
                bigo_a1 = leftover_after
            else:
                bigo_a1 = 0

            sql_ins = """
                INSERT INTO lot_hst
                (lot_no, prg_cd, lot_seq, amt, man_cd, jepum_cd,
                 work_dt, bigo_1, bigo_a1)
                VALUES
                (?, '180', ?, ?, ?, ?,
                 CONVERT(varchar, GETDATE(), 112), ?, ?)
            """
            cur.execute(sql_ins, (
                lot_no, i, use_qty, man_cd, jepum_cd, bin_no, bigo_a1
            ))

            leftover -= use_qty
            if leftover < 0:
                leftover = 0

        conn.commit()
        conn.close()
        return jsonify({"message": "Taping 공정 등록 성공"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# 파일 업로드 API
@file_insert_bp.route("/upload", methods=["POST"])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    
    file = request.files['file']

    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file:
        filename = secure_filename(file.filename)
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        
        try:
            file.save(file_path)
            return jsonify({
                "message": "File successfully uploaded",
                "filename": filename,
                "path": file_path
            }), 200
        except Exception as e:
            return jsonify({"error": f"Failed to save file: {str(e)}"}), 500
    
    return jsonify({"error": "An unknown error occurred"}), 500

@report_insert_bp.route("/checklist", methods=["POST"])
def save_report_checklist():
    """
    모바일 앱에서 전송된 점검 결과(체크리스트 포함) 전체를 DB에 저장합니다.
    - 새로운 스키마에 따라, 체크리스트의 각 항목을 INSPECTION_MST 테이블에 개별 행으로 저장합니다.
    """
    # 1. DB 연결을 위한 v_db 파라미터 받기
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    conn = None  # DB 연결 객체 초기화
    try:
        # 2. 클라이언트에서 보낸 JSON 데이터 추출
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON 데이터가 필요합니다."}), 400

        # 3. 데이터 추출 및 유효성 검사
        code = data.get('code')  # 설비 코드 (필수)
        checklist = data.get('checklist') # 점검 항목 리스트 (필수)
        photo_filename = data.get('photo_filename') # 사진 파일명 (옵션)

        # 필수 값 체크
        if not all([code, checklist]):
            return jsonify({"error": "필수 데이터(code, checklist)가 누락되었습니다."}), 400
        
        if not isinstance(checklist, list) or len(checklist) == 0:
            return jsonify({"error": "checklist는 비어있지 않은 리스트여야 합니다."}), 400

        # 4. DB 작업 시작
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500
        
        cur = conn.cursor()

        # 5. INSPECTION_MST 테이블에 여러 행을 삽입할 SQL 준비
        sql = """
            INSERT INTO INSPECTION_MST (code, question, answer, photo_filename)
            VALUES (?, ?, ?, ?)
        """
        
        # executemany에 사용할 파라미터 리스트 생성
        # 모든 점검 항목 행에 동일한 code, photo_filename이 들어갑니다.
        params_list = [
            (
                code, 
                item['question'].encode('euc-kr'), 
                item['answer'].encode('euc-kr'),
                photo_filename if photo_filename else None
            )
            for item in checklist
        ]

        # 6. executemany를 사용하여 모든 점검 항목을 트랜잭션으로 한 번에 삽입
        cur.executemany(sql, params_list)

        # 7. 모든 작업이 성공하면 commit
        conn.commit()

        return jsonify({
            "message": "점검 리포트가 성공적으로 저장되었습니다."
        }), 201

    except Exception as e:
        # 8. 에러 발생 시 rollback하여 데이터 일관성 유지
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        # 9. 작업 완료 후 DB 연결 종료
        if conn:
            conn.close()