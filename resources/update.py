# resources/suju_update.py
from flask import Blueprint, jsonify, request
from db import get_db_connection

suju_update_bp = Blueprint("suju_update", __name__)
# 이미 suju_update_bp가 존재하므로, stock_update_bp 추가
stock_update_bp = Blueprint("stock_update_bp", __name__)

etc_update_bp = Blueprint("etc_update", __name__)

@suju_update_bp.route("/update", methods=["PUT"])
def suju_update():
    # 쿼리스트링으로 전달되는 v_db 파라미터 필수
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    # 수정할 대상 식별자는 suju_cd
    suju_cd = data.get("suju_cd")
    # 수정할 필드들 (필수: suju_dt, out_dt_to, jepum_cd, vender_cd, amt)
    suju_dt   = data.get("suju_dt")    # 예: "2025-03-06"
    out_dt_to = data.get("out_dt_to")  # 예: "2025-03-06"
    jepum_cd  = data.get("jepum_cd")
    vender_cd = data.get("vender_cd")
    amt       = data.get("amt")
    # 선택적 필드들: 비고, process_cd (기본값 "01")
    bigo      = data.get("bigo", "").encode('euc-kr')
    process_cd = data.get("process_cd", "01")

    if not suju_cd or not suju_dt or not out_dt_to or not jepum_cd or not vender_cd or not amt:
        return jsonify({"error": "필수 필드 누락"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()

        # INSERT 코드와 동일하게 날짜에서 '-' 제거 (예: "2025-03-06" -> "20250306")
        suju_dt = suju_dt.replace("-", "")
        out_dt_to = out_dt_to.replace("-", "")

        query = """
            UPDATE suju_mst
            SET suju_dt = ?,
                out_dt_to = ?,
                jepum_cd = ?,
                vender_cd = ?,
                amt = ?,
                bigo = ?,
                process_cd = ?
            WHERE suju_cd = ?
        """
        cur.execute(query, (suju_dt, out_dt_to, jepum_cd, vender_cd, amt, bigo, process_cd, suju_cd))
        conn.commit()
        conn.close()
        return jsonify({"message": "UPDATE success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@stock_update_bp.route("/update", methods=["PUT"])
def stock_update():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    # inout_no가 PK(고유키) 역할
    inout_no = data.get("inout_no")
    inout_dt = data.get("inout_dt")    # "2025-03-06" 형태
    jepum_cd = data.get("jepum_cd")
    vender_cd = data.get("vender_cd")
    confirm_amt = data.get("confirm_amt")
    bigo = data.get("bigo", "").encode('euc-kr')

    if not inout_no or not inout_dt or not jepum_cd or not confirm_amt:
        return jsonify({"error": "필수 필드 누락"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        # inout_dt에서 '-' 제거
        inout_dt = inout_dt.replace("-", "")

        sql = """
            UPDATE stock_mst
            SET inout_dt = ?,
                jepum_cd = ?,
                vender_cd = ?,
                confirm_amt = ?,
                bigo = ?
            WHERE inout_no = ?
        """
        cur.execute(sql, (inout_dt, jepum_cd, vender_cd, confirm_amt, bigo, inout_no))
        conn.commit()
        conn.close()
        return jsonify({"message": "출고 UPDATE success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@etc_update_bp.route("/test-result", methods=["PUT"])
def update_test_result():
    """
    PUT /api/update/test_result?v_db=16_UR
    Body:
    {
      "lot_no": "T123",
      "jepum_cd": "JP002",
      "amt": 200,
      "man_cd": "M002",
      "bin_no": "BIN-9",
      "work_dt": "2025-04-02"
    }
    """
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
    lot_no2  = data.get("lot_no2")
    dev_no   = data.get("dev_no")

    if not all([lot_no, jepum_cd, amt, man_cd, work_dt, lot_no2, dev_no]):
        return jsonify({"error": "필수 필드 누락"}), 400

    try:
        work_dt_str = work_dt.replace("-", "")

        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500

        cur = conn.cursor()
        query = """
            UPDATE lot_hst
            SET jepum_cd = ?,
                amt      = ?,
                man_cd   = ?,
                bigo_1   = ?,
                work_dt  = ?,
                lot_no2  = ?,
                dev_no   = ?
            WHERE lot_no = ?
              AND prg_cd = '170'
        """
        cur.execute(query, (jepum_cd, amt, man_cd, bin_no, work_dt_str, lot_no2, dev_no, lot_no))
        conn.commit()
        conn.close()

        return jsonify({"message": "TEST 실적 수정 성공"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@etc_update_bp.route("/tapping-result", methods=["PUT"])
def update_tapping():
    """
    Taping 공정 수정 API
    - (예시) 기존 180 데이터(릴 분할된 lot_hst)를 모두 삭제한 후,
      새로 전달된 amt, reel_count, reel_min_amt 로 다시 분할 insert한다.
    - "작업자(man_cd), BIN NO(bin_no) 등도 함께 변경"
    """
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON Body가 필요합니다."}), 400

    # 필드 추출 (필수값 확인)
    lot_no       = data.get("lot_no")
    amt          = data.get("amt")           # 새로 수정할 총수량
    reel_count   = data.get("reel_count")    # 새로 수정할 릴 개수
    reel_min_amt = data.get("reel_min_amt")  # 릴당 수량
    man_cd       = data.get("man_cd")        # 작업자 (한글 인코딩 주의)
    bin_no       = data.get("bin_no")        # BIN NO
    jepum_cd     = data.get("jepum_cd", "")  # 제품코드(필요하다면)

    if not lot_no:
        return jsonify({"error": "lot_no 누락"}), 400
    
    # 반드시 reel_count, reel_min_amt 등도 모두 보내줘야 "다시 분할" 처리가 가능하므로
    # 여기서는 amt, reel_count, reel_min_amt, man_cd 가 모두 필요하다고 가정.
    if not all([amt, reel_count, reel_min_amt, man_cd]):
        return jsonify({"error": "필수 필드(amt, reel_count, reel_min_amt, man_cd) 누락"}), 400

    # man_cd 가 한글이면 euc-kr 인코딩이 필요하다는 기존 요구사항이 있었다면
    # 아래처럼 encode 해주되, DB가 어떤 인코딩인지 확인 필요
    man_cd_encoded = man_cd.encode('euc-kr')  # 혹은 DB가 UTF-8이면 그냥 사용

    try:
        # DB 연결
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500
        cur = conn.cursor()

        # (1) LOT 존재/중복검사 (170 or 기존 180 여부 등) - 필요하다면 로직 추가
        #     예: 170 등록 여부 검사 (insert때와 동일)
        check_170 = """
            SELECT COUNT(*) FROM lot_hst
            WHERE lot_no=? AND prg_cd='170'
        """
        cur.execute(check_170, (lot_no,))
        if cur.fetchone()[0] == 0:
            conn.close()
            return jsonify({"error": "TEST(170) 등록 이력이 없는 LOT"}), 400

        # (2) 기존 180 데이터 삭제
        delete_sql = """
            DELETE FROM lot_hst
            WHERE lot_no = ?
              AND prg_cd = '180'
        """
        cur.execute(delete_sql, (lot_no,))

        # (3) 새로운 180 데이터 (릴 분할)로 재-insert
        leftover = amt  # 남은 수량을 처음엔 전체로 둠
        for i in range(1, reel_count + 1):
            # 이번 릴에 할당할 수량 결정
            if leftover > reel_min_amt:
                use_qty = reel_min_amt
            else:
                use_qty = leftover

            # 마지막 릴이라면? -> 남은 leftover를 빼고 그 값을 bigo_a1로 기록
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
                lot_no, i, use_qty, man_cd_encoded, jepum_cd,
                bin_no, bigo_a1
            ))

            leftover -= use_qty
            if leftover < 0:
                leftover = 0

        conn.commit()
        conn.close()

        return jsonify({"message": "Taping 수정(재분할) 성공"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
