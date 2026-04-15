# resources/select.py
from flask import Blueprint, send_from_directory, jsonify, request
import json
import os
import traceback  # <--- 1. 이 줄을 추가하세요.
from db import get_db_connection, DecimalEncoder

# 거래처(고객) 조회용 블루프린트
vender_select_bp = Blueprint("vender_select_bp", __name__) # 거래처 조회용 블루프린트...........1
# 제품 조회용 블루프린트
jepum_select_bp = Blueprint("jepum_select_bp", __name__) # 제품 조회용 블루프린트...........2
# 재고 조회용 블루프린트
stock_select_bp = Blueprint("stock_select_bp", __name__) # 재고 조회용 블루프린트...........3
# 주문 조회용 블루프린트
suju_select_bp = Blueprint("suju_select_bp", __name__) # 주문 조회용 블루프린트...........4
equip_select_bp = Blueprint("equip_select_bp", __name__) #설비 조회 블루프린트..............5
segsan_select_bp = Blueprint("segsan_select_bp", __name__) # 생산 조회 블루프린트...........6
etc_select_bp = Blueprint("etc_select_bp", __name__) # 기타 조회 블루프린트...........7
smart_select_bp = Blueprint("smart_select_bp", __name__) # 설비 데이터 수집 블루프린트...........8
data_select_bp = Blueprint("data_select_bp", __name__) # 데이터 분석 블루프린트...........9


report_select_bp = Blueprint("report_select_bp", __name__) # 리포트 조회 블루프린트...........10
file_select_bp = Blueprint("file_select_bp", __name__) # 파일 조회 블루프린트...........11


# 거래처 조회용 API.............1
# /all 거래처 전체 조회
@vender_select_bp.route("/all", methods=["GET"])
def vender_select_all():
    """
    거래처별 조회 예시
    GET /api/select/vender/all?vendor=16_UR
    v_db 파라미터를 통해 어느 업체(스키마)로 접속할지 결정.
    (예: 16_UR 거래처 데이터 조회)
    """
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        # 거래처 테이블 조회 예시 (테이블 이름은 예시)
        query = """
            SELECT vender_cd, vender_nm, city, address1, tel
            FROM vender_code
            ORDER BY vender_cd
        """
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "vender_cd": row[0],
                "vender_nm": row[1],
                "city": row[2],
                "address1": row[3],
                "tel": row[4]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        # --- 2. 아래 두 줄을 추가하세요. ---
        print(f"!!! ERROR at {request.path} (v_db={v_db}) !!!")
        print(traceback.format_exc()) 
        # --- 여기까지 ---
        return jsonify({"error": str(e)}), 500

# /out 매출처 전체 조회
@vender_select_bp.route("/out", methods=["GET"])
def vender_select_out():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        # 거래처 테이블 조회 예시 (테이블 이름은 예시)
        query = """
            SELECT vender_cd, vender_nm, city, address1, tel
            FROM vender_code
            WHERE tab_gbn_cd = '01'
            ORDER BY vender_cd
        """
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "vender_cd": row[0],
                "vender_nm": row[1],
                "city": row[2],
                "address1": row[3],
                "tel": row[4]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500







# 제품 조회용 API.............2

# /all 제품 전체 조회
@jepum_select_bp.route("/all", methods=["GET"])
def jepum_select_all():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        # 제품 테이블 조회 예시 (테이블 이름은 예시)
        query = """
            SELECT jepum_cd, jepum_nm
            FROM jepum_code
            ORDER BY jepum_cd
        """
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "jepum_cd": row[0],
                "jepum_nm": row[1]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# /jepum 완제품 조회
@jepum_select_bp.route("/jepum", methods=["GET"])
def jepum_select_jepum():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        # 제품 테이블 조회 예시 (테이블 이름은 예시)
        query = """
            SELECT jepum_cd, jepum_nm
            FROM jepum_code
            WHERE tab_gbn_cd = '01'
            ORDER BY jepum_cd
        """
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "jepum_cd": row[0],
                "jepum_nm": row[1]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500






# 재고 조회용 API.............3

# jepum 제품별 재고 조회
@stock_select_bp.route("/jepum", methods=["GET"])
def stock_select_jepum():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        # 제품 테이블 조회 예시 (테이블 이름은 예시)
        query = """
            SELECT a.jepum_cd, b.jepum_nm, b.color, a.amt
            FROM stock_last a
            LEFT OUTER JOIN jepum_code b
            ON a.jepum_cd = b.jepum_cd
            WHERE a.amt > 0
            ORDER BY a.jepum_cd
        """
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "jepum_cd": row[0],
                "jepum_nm": row[1],
                "spec": row[2],
                "amt": row[3]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

# 완제품 출고 조회
@stock_select_bp.route("/jepum-out", methods=["GET"])
def stock_select_jepum_out():
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")  # 예: "20230201"
    to_dt = request.args.get("to_dt")      # 예: "20230301"

    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        # 제품 테이블 조회 예시 (테이블 이름은 예시)
        query = """
            SELECT a.inout_no, a.inout_dt, a.jepum_cd, b.jepum_nm, a.confirm_amt, a.vender_cd, c.vender_nm, a.stock_cd_from, a.stock_cd_to
            FROM stock_mst a
            LEFT OUTER JOIN jepum_code b ON a.jepum_cd = b.jepum_cd
            LEFT OUTER JOIN vender_code c ON a.vender_cd = c.vender_cd            
            WHERE a.inout_gbn = 'KZ' AND a.inout_dt >= ? AND a.inout_dt <= ?
            ORDER BY a.inout_dt desc
        """
        cur.execute(query, (from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "inout_no": row[0],
                "inout_dt": row[1],
                "jepum_cd": row[2],
                "jepum_nm": row[3],
                "confirm_amt": row[4],
                "vender_cd": row[5],
                "vender_nm": row[6],
                "stock_cd_from": row[7],
                "stock_cd_to": row[8]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# 주문 조회용 API.............4

# /all 주문 전체 조회
@suju_select_bp.route("/all", methods=["GET"])
def suju_select_all():
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")  # 예: "20230201"
    to_dt = request.args.get("to_dt")      # 예: "20230301"

    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    if not from_dt or not to_dt:
        return jsonify({"error": "from_dt, to_dt parameter is required"}), 400


    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500
        
        cur = conn.cursor()
        query = """
            SELECT a.suju_cd, a.suju_dt, a.out_dt_to, a.jepum_cd, b.jepum_nm, a.vender_cd, c.vender_nm, a.amt, a.bigo, a.process_cd
            FROM suju_mst a
            LEFT OUTER JOIN jepum_code b ON a.jepum_cd = b.jepum_cd
            LEFT OUTER JOIN vender_code c ON a.vender_cd = c.vender_cd
            WHERE a.suju_dt >= ? AND a.suju_dt <= ?
            ORDER BY a.write_dt
        """
        cur.execute(query, (from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()
        
        data = []
        for row in rows:
            data.append({
                "suju_cd": row[0],
                "suju_dt": row[1],
                "out_dt_to": row[2],
                "jepum_cd": row[3],
                "jepum_nm": row[4],
                "vender_cd": row[5],
                "vender_nm": row[6],
                "amt": row[7],
                "bigo": row[8],
                "process_cd": row[9]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


#설비 조회 블루프린트..............5

# mst 설비조회
@equip_select_bp.route("/mst", methods=["GET"])
def equip_select_mst():
    v_db = request.args.get("v_db")

    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500
        
        cur = conn.cursor()
        query = """
            SELECT a.equip_cd, a.equip_nm
            FROM equip_mst a
            WHERE a.use_yn = '01'
        """
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()
        
        data = []
        for row in rows:
            data.append({
                "equip_cd": row[0],
                "equip_nm": row[1]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# 설비 점검내역 조회 API 추가
@equip_select_bp.route("/inspect", methods=["GET"])
def equip_select_inspect():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500
        cur = conn.cursor()

        # 설비 마스터와 점검 내역을 LEFT JOIN하여, 해당 설비의 가장 최근 점검일자를 구하고,
        # 점검 경과일수는 최근 점검일이 있을 경우 오늘과의 일수 차이를, 없으면 '최근점검내역없음'을 표시합니다.
        query = """
            SELECT a.equip_cd, a.equip_nm, b.equip_dt AS equip_dt,
                   CASE 
                       WHEN b.equip_dt IS NULL THEN N'최근점검내역없음'
                       ELSE CONVERT(varchar, DATEDIFF(day, b.equip_dt, GETDATE())) + N'일 경과'
                   END AS diff_dt
            FROM equip_mst a
            LEFT JOIN (
                SELECT equip_cd, MAX(equip_dt) AS equip_dt
                FROM equip_hst
                GROUP BY equip_cd
            ) b ON a.equip_cd = b.equip_cd
            WHERE a.use_yn = '01'
        """

        cur.execute(query)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "equip_cd": row[0],
                "equip_nm": row[1],
                "equip_dt": row[2],
                "diff_dt": row[3]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@segsan_select_bp.route("/process", methods=["GET"])
def segsan_process_result():
    """
    특정 공정코드(prg_cd)에 대한 실적을 날짜 범위로 조회한다.
    예) /api/select/segsan/process?v_db=16_UR&prg_cd=110&from_dt=20250301&to_dt=20250331
    """
    v_db = request.args.get("v_db")
    prg_cd = request.args.get("prg_cd")     # 예: "110" (다이본드), "120" (W/B) 등
    from_dt = request.args.get("from_dt")   # 예: "20250301"
    to_dt   = request.args.get("to_dt")     # 예: "20250331"

    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400
    if not prg_cd:
        return jsonify({"error": "prg_cd 파라미터가 필요합니다."}), 400
    if not from_dt or not to_dt:
        return jsonify({"error": "from_dt, to_dt 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500
        cur = conn.cursor()

        # segsan_mst에서 prg_cd가 요청 파라미터로 넘어온 값과 일치하는 행만 조회
        # jepum_code와 JOIN하여 제품명을 가져오고,
        # segsan_dt는 "yy-MM-dd" 형태로 변환
        query = """
            SELECT s.jepum_cd, j.jepum_nm, s.amt,
                   SUBSTRING(CONVERT(varchar, s.segsan_dt, 112), 3, 2) + '-' +
                   SUBSTRING(CONVERT(varchar, s.segsan_dt, 112), 5, 2) + '-' +
                   SUBSTRING(CONVERT(varchar, s.segsan_dt, 112), 7, 2) AS segsan_dt,
                   s.lot_no
            FROM segsan_mst s
            LEFT JOIN jepum_code j ON s.jepum_cd = j.jepum_cd
            WHERE s.prg_cd = ?
              AND s.segsan_dt >= ?
              AND s.segsan_dt <= ?
            ORDER BY s.segsan_dt DESC
        """
        cur.execute(query, (prg_cd, from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "jepum_cd": row[0],
                "jepum_nm": row[1],
                "amt":      row[2],
                "segsan_dt": row[3],
                "lot_no":    row[4]
            })

        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# TEST 공정 결과 조회 (SELECT) -> 16_UR 업체에서만 사용
@etc_select_bp.route("/test-result", methods=["GET"])
def select_test_result():
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")  # 예: "20250301"
    to_dt = request.args.get("to_dt")      # 예: "20250331"
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400
    if not from_dt or not to_dt:
        return jsonify({"error": "from_dt and to_dt parameters are required"}), 400
    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        query = """
            SELECT h.lot_no, h.jepum_cd, j.jepum_nm, h.amt, h.man_cd, h.bigo_1, h.work_dt, h.lot_no2, h.dev_no
            FROM lot_hst h
            LEFT JOIN jepum_code j ON h.jepum_cd = j.jepum_cd
            WHERE h.prg_cd = '170'
              AND h.work_dt >= ? AND h.work_dt <= ?
            ORDER BY h.work_dt DESC
        """
        cur.execute(query, (from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()
        data = []
        for row in rows:
            data.append({
                "lot_no": row[0],
                "jepum_cd": row[1],
                "jepum_nm": row[2],
                "amt": row[3],
                "man_cd": row[4],
                "bigo_1": row[5],
                "work_dt": row[6],
                "lot_no2": row[7],
                "dev_no":row[8]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# LOT NO를 통한 제품조회 -> 16_UR 업체에서만 사용
@etc_select_bp.route("/lot_no_inform", methods=["GET"])
def select_lot_no_inform():
    v_db = request.args.get("v_db")
    lot_no2 = request.args.get("lot_no2")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400
    if not lot_no2:
        return jsonify({"error": "lot_no2 parameters are required"}), 400
    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        # 📌 [수정] 쿼리의 WHERE 절에 OR 조건을 추가
        query = """
            SELECT TOP 1 h.jepum_cd, j.jepum_nm, l.bigo39, l.bigo40
            FROM lot_mst h
            LEFT JOIN jepum_code j ON h.jepum_cd = j.jepum_cd
            LEFT JOIN lot_bigo l ON h.lot_no = l.lot_no
            WHERE 
                SUBSTRING(h.lot_no, 2, LEN(h.lot_no)) = ? 
                OR 
                (ISNULL(l.bigo39, '') + '-' + ISNULL(l.bigo40, '')) = ?
        """
        
        # 📌 [수정] 파라미터를 2개 전달 (물음표가 2개 있으므로)
        cur.execute(query, (lot_no2, lot_no2))
        rows = cur.fetchall()
        conn.close()
        data = []
        for row in rows:
            data.append({
                "jepum_cd": row[0],
                "jepum_nm": row[1],
                "bigo39": row[2],
                "bigo40": row[3]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# TEST 공정 작업자 리스트 조회 -> 16_UR 업체에서만 사용
@etc_select_bp.route("/test_man_cd", methods=["GET"])
def select_test_man_cd():
    v_db = request.args.get("v_db")
    dept_cd = request.args.get("dept_cd")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400
    if not dept_cd:
        return jsonify({"error": "dept_cd parameter is required"}), 400
    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        query = """
            SELECT a.emp_nmk
            FROM hmtperson a
            WHERE a.dept_cd = ?
        """
        cur.execute(query, (dept_cd))
        rows = cur.fetchall()
        conn.close()
        data = []
        for row in rows:
            data.append({
                "emp_nmk": row[0]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@etc_select_bp.route("/tapping-result", methods=["GET"])
def select_tapping_result():
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")
    to_dt   = request.args.get("to_dt")

    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400
    if not from_dt or not to_dt:
        return jsonify({"error": "from_dt, to_dt 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500

        cur = conn.cursor()
        query = """
            SELECT
                h.work_dt,
                h.lot_no,
                MAX(h.lot_seq) AS lot_seq,
                h.jepum_cd,
                MAX(c.jepum_nm) AS jepum_nm,
                MAX(h.amt) AS amt,
                MAX(h.man_cd) AS man_cd,
                MAX(h.bigo_1) AS bin_no,               
                MAX(h.bigo_a1) AS leftover,
                MAX(h.bigo_3) AS bigo_3,  -- 추가: 작업 NO
                MAX(h.bigo_4) AS bigo_4   -- 추가: 장비명
            FROM lot_hst h
            LEFT JOIN jepum_code c ON h.jepum_cd = c.jepum_cd
            WHERE h.prg_cd = '180'
            AND h.work_dt >= ?
            AND h.work_dt <= ?
            GROUP BY
                h.work_dt,
                h.lot_no,
                h.jepum_cd
            ORDER BY
                h.work_dt DESC,
                h.lot_no
        """
        cur.execute(query, (from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()

        # 4) JSON 변환
        data = []
        for row in rows:
            data.append({
                "work_dt":   row[0],  
                "lot_no":    row[1],
                "lot_seq":   row[2],
                "jepum_cd":  row[3],
                "jepum_nm":  row[4],
                "amt":       row[5],
                "man_cd":    row[6],
                "bigo_1":    row[7],  
                "bigo_a1":   row[8],  
                "bigo_3":    row[9],  # 추가: 작업 NO
                "bigo_4":    row[10], # 추가: 장비명
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@etc_select_bp.route("/tapping-check-lot", methods=["GET"])
def check_lot_for_tapping():
    v_db = request.args.get("v_db")
    lot_no = request.args.get("lot_no")
    if not v_db:
        return jsonify({"error": "v_db 파라미터 필요"}), 400
    if not lot_no:
        return jsonify({"error": "lot_no 파라미터 필요"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500
        cur = conn.cursor()

        # 1) 170 공정 여부 체크
        # 📌 [수정] SELECT 절에 'amt' 를 추가했습니다.
        sql_170 = """
            SELECT jepum_cd, bigo_1, amt
            FROM lot_hst
            WHERE lot_no = ?
              AND prg_cd = '170'
        """
        cur.execute(sql_170, (lot_no,))
        row_170 = cur.fetchone()
        if not row_170:
            conn.close()
            return jsonify({"error": "TEST(170) 등록 이력이 없는 LOT NO"}), 400

        jepum_cd_170 = row_170[0]
        bin_no_170   = row_170[1]
        amt_170      = row_170[2] # 📌 [추가] 조회된 수량 변수에 담기

        # 2) 이미 Taping(180) 등록 여부
        sql_180 = """
            SELECT COUNT(*)
            FROM lot_hst
            WHERE lot_no = ?
              AND prg_cd = '180'
              AND lot_seq = 1
        """
        cur.execute(sql_180, (lot_no,))
        cnt_180 = cur.fetchone()[0]
        if cnt_180 > 0:
            conn.close()
            return jsonify({"error": "이미 Tapping(180)에 등록된 LOT NO"}), 400

        conn.close()
        # 📌 [수정] JSON 응답에 'amt' 도 함께 넘겨줍니다.
        return jsonify({
            "message": "OK",
            "jepum_cd": jepum_cd_170,
            "bin_no": bin_no_170,
            "amt": amt_170 
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# 설비 데이터 수집 블루프린트...........8
@smart_select_bp.route("/smart-log", methods=["GET"])
def smart_select_equip():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        # 제품 테이블 조회 예시 (테이블 이름은 예시)
        query = """
            SELECT ymd+hh+mm as ymdhhmm, auto_id, col_1, col_2, col_3, col_4, bigo
            FROM smart_log
            ORDER BY ymd, hh, mm DESC
        """
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "ymdhhmm": row[0],
                "auto_id": row[1],
                "col_1": row[2],
                "col_2": row[3],
                "col_3": row[4],
                "col_4": row[5],
                "bigo": row[6]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

# 데이터 분석 블루프린트...........9

#.....smart공정목록...................
@data_select_bp.route("/smart-prg-cd", methods=["GET"])
def get_smart_prg_cd():
    """
    smart_prg_mst 테이블에서 모든 공정 목록을 조회하여 반환합니다.
    프론트엔드에서 공정 선택 UI를 만드는 데 사용됩니다.
    """
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        
        query = "SELECT prg_cd, prg_nm FROM smart_prg_mst ORDER BY prg_cd"
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()

        # 결과를 dictionary list 형태로 가공
        process_list = [{"prg_cd": row[0], "prg_nm": row[1]} for row in rows]
        
        return jsonify(process_list), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 기기별 비가동 시간 분석...........
@data_select_bp.route("/equip-down-time", methods=["GET"])
def get_equip_down_time():
    """
    mon_01_v 뷰의 모든 데이터 컬럼(271개)을 사용하여,
    선택된 공정과 날짜에 해당하는 설비들의 전체 데이터를 반환합니다.
    """
    # 1. 프론트엔드로부터 파라미터 받기 (v_db, ymd, prg_cd)
    v_db = request.args.get("v_db")
    ymd = request.args.get("ymd")
    prg_cd = request.args.get("prg_cd")

    if not all([v_db, ymd, prg_cd]):
        return jsonify({"error": "v_db, ymd, prg_cd parameters are all required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()

        # 2. SQL 쿼리 수정: v.* 를 사용하여 mon_01_v 뷰의 모든 컬럼을 선택
        query = """
            SELECT
                m.auto_nm,
                v.*
            FROM
                mon_01_v v
            JOIN
                smart_mst m ON v.auto_id = m.auto_id
            WHERE
                v.ymd = ?
                AND m.prg_cd = ?
            ORDER BY
                m.auto_nm
        """
        
        cur.execute(query, (ymd, prg_cd))
        rows = cur.fetchall()
        conn.close()

        # 3. 조회 결과를 최종 응답 데이터 형식으로 조합
        result_data = []

        for row in rows:
            # 'SELECT m.auto_nm, v.*' 쿼리의 결과 row의 구조는 다음과 같습니다:
            # row[0]: m.auto_nm (설비명)
            # row[1]: v.auto_id (뷰의 첫번째 컬럼)
            # row[2]: v.ymd (뷰의 두번째 컬럼)
            # row[3] 부터 끝까지: v.col_001, v.col_002, ... 등 271개의 데이터 컬럼
            
            # 4번째 요소(인덱스 3)부터 끝까지 슬라이싱하여 271개의 데이터 컬럼을 리스트로 만듭니다.
            status_list = list(row[3:])
            
            result_data.append({
                "device": row[0],
                "date": row[2],
                "status": status_list # 이 리스트는 이제 271개의 요소를 가집니다.
            })
        
        return jsonify(result_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 공정-기기별 생산시간 분석.........................
@data_select_bp.route("/process-equip", methods=["GET"])
def get_process_uptime():
    """
    선택된 기간(from_dt, to_dt)과 공정(prg_cd)에 해당하는
    모든 기기들의 총 가동시간과 총 비가동시간을 집계하여 반환합니다.
    """
    # 1. 프론트엔드로부터 파라미터 받기 (v_db, prg_cd, from_dt, to_dt)
    v_db = request.args.get("v_db")
    prg_cd = request.args.get("prg_cd")
    from_dt = request.args.get("from_dt")
    to_dt = request.args.get("to_dt")

    if not all([v_db, prg_cd, from_dt, to_dt]):
        return jsonify({"error": "v_db, prg_cd, from_dt, to_dt parameters are all required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()

        # 2. mon_02_v 뷰와 smart_mst 테이블을 JOIN하여 최종 데이터 집계
        #    - v.ymd BETWEEN ? AND ? : 기간 필터링
        #    - m.prg_cd = ? : 공정 필터링
        #    - GROUP BY m.auto_nm : 기기별로 합산
        query = """
            SELECT
                m.auto_nm,
                SUM(v.green) AS green,
                SUM(v.red) AS red
            FROM
                mon_02_v v
            JOIN
                smart_mst m ON v.auto_id = m.auto_id
            WHERE
                m.prg_cd = ?
                AND v.ymd BETWEEN ? AND ?
            GROUP BY
                m.auto_nm
            ORDER BY
                m.auto_nm;
        """
        
        cur.execute(query, (prg_cd, from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()

        # 3. 조회 결과를 프론트엔드에서 사용하기 좋은 JSON 형식으로 가공
        result_data = []
        for row in rows:
            result_data.append({
                "device": row[0],
                "operation_time": row[1],
                "non_operation_time": row[2] # 화면의 '비가동시간'에 해당
            })
        
        return jsonify(result_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 제품별 불량률 분석.........................
@data_select_bp.route("/jepum-defect-rate", methods=["GET"])
def get_jepum_defect_rate():
    """
    (진짜 최종) 기간으로 조회하여 '제품별'
    총생산, 불량, 불량률 데이터를 집계하여 반환합니다.
    """
    # 1. 프론트엔드로부터 파라미터 받기
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")
    to_dt = request.args.get("to_dt")

    if not all([v_db, from_dt, to_dt]):
        return jsonify({"error": "v_db, from_dt, to_dt parameters are all required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()

        # 2. 보내주신 '진짜 최종' 쿼리
        query = """
            SELECT vv_t.jepum_cd  AS 'jepum_cd',
                Sum(vv_t.amt_ok)  AS 'amt_ok',
                Sum(vv_t.amt_err) AS 'amt_err'
            FROM   (SELECT v_t.lot_no                 AS 'lot_no',
                        v_t.jepum_cd               AS 'jepum_cd',
                        Sum(Isnull(v_t.amt_ok, 0)) AS 'amt_ok',
                        Sum(Isnull(v_b.amt, 0))    AS 'amt_err'
                    FROM   (SELECT a.lot_no   AS 'lot_no',
                                a.jepum_cd AS 'jepum_cd',
                                Sum(a.amt) AS 'amt_ok'
                            FROM   segsan_mst a
                            WHERE  a.segsan_dt between ? AND ? AND a.prg_cd = '110'
                            GROUP  BY a.lot_no, a.jepum_cd) v_t
                        LEFT OUTER JOIN banpum_mst v_b ON v_t.lot_no = v_b.lot_no AND v_t.jepum_cd = v_b.jepum_cd
                    GROUP  BY v_t.lot_no,
                            v_t.jepum_cd) vv_t
            GROUP  BY vv_t.jepum_cd 
        """
        
        params = (from_dt, to_dt)
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()

        result_data = []
        for row in rows:
            result_data.append({
                "jepum_cd": row[0],
                "ok": int(row[1] or 0),
                "err": int(row[2] or 0)
            })
        
        return jsonify(result_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# 기기별 수율 분석
@data_select_bp.route("/line-defect-rate", methods=["GET"])
def get_line_defect_rate():
    # 1. 프론트엔드로부터 파라미터 받기
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")
    to_dt = request.args.get("to_dt")

    if not all([v_db, from_dt, to_dt]):
        return jsonify({"error": "v_db, from_dt, to_dt parameters are all required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()

        query = """
            SELECT vv_t.line      AS 'line',
                Sum(vv_t.amt_ok)  AS 'ok',
                Sum(vv_t.amt_err) AS 'err'
            FROM   (SELECT v_t.lot_no              AS 'lot_no',
                        v_t.jepum_cd               AS 'jepum_cd',
                        Sum(Isnull(v_t.amt_ok, 0)) AS 'amt_ok',
                        Sum(Isnull(v_b.amt, 0))    AS 'amt_err',
                        MAX(CASE WHEN ISNUMERIC(v_t.line) = 1 THEN CAST(v_t.line AS INT) ELSE 1 END) AS 'line'
                    FROM   (SELECT a.lot_no  AS 'lot_no',
                                a.jepum_cd   AS 'jepum_cd',
                                Sum(a.amt)   AS 'amt_ok',
                                Max(a.line)  AS 'line'
                            FROM   segsan_mst a
                            WHERE  a.segsan_dt between ? and ? AND a.prg_cd = '110'
                            GROUP  BY a.lot_no, a.jepum_cd) v_t
                        LEFT OUTER JOIN banpum_mst v_b ON v_t.lot_no = v_b.lot_no AND v_t.jepum_cd = v_b.jepum_cd
                    GROUP  BY v_t.lot_no,
                            v_t.jepum_cd) vv_t
            GROUP  BY vv_t.line
            ORDER  BY vv_t.line
        """
        
        params = (from_dt, to_dt)
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()

        result_data = []
        for row in rows:
            result_data.append({
                "line": row[0],
                "ok": int(row[1] or 0),
                "err": int(row[2] or 0)
            })
        
        return jsonify(result_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 제품-기기별 수율 분석
@data_select_bp.route("/jepum-equip-defect-rate", methods=["GET"])
def get_jepum_equip_defect_rate():
    """
    (null 처리 수정) 기간 필터가 적용된 제품별/기기별 데이터를
    피벗하여 가공 후 반환합니다.
    """
    # ... 이전과 동일한 파라미터 받기 및 DB 연결 로직 ...
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")
    to_dt = request.args.get("to_dt")

    if not all([v_db, from_dt, to_dt]):
        return jsonify({"error": "v_db, from_dt, to_dt parameters are all required"}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # 쿼리는 이전과 동일합니다.
        query = """
            SELECT jepum_nm,
                   SUM(CASE WHEN dev_no = 1 THEN amt_ok ELSE 0 END) AS dev1_ok,
                   SUM(CASE WHEN dev_no = 1 THEN amt_err ELSE 0 END) AS dev1_err,
                   SUM(CASE WHEN dev_no = 2 THEN amt_ok ELSE 0 END) AS dev2_ok,
                   SUM(CASE WHEN dev_no = 2 THEN amt_err ELSE 0 END) AS dev2_err,
                   SUM(CASE WHEN dev_no = 3 THEN amt_ok ELSE 0 END) AS dev3_ok,
                   SUM(CASE WHEN dev_no = 3 THEN amt_err ELSE 0 END) AS dev3_err,
                   SUM(CASE WHEN dev_no = 4 THEN amt_ok ELSE 0 END) AS dev4_ok,
                   SUM(CASE WHEN dev_no = 4 THEN amt_err ELSE 0 END) AS dev4_err,
                   SUM(CASE WHEN dev_no = 5 THEN amt_ok ELSE 0 END) AS dev5_ok,
                   SUM(CASE WHEN dev_no = 5 THEN amt_err ELSE 0 END) AS dev5_err,
                   SUM(CASE WHEN dev_no = 6 THEN amt_ok ELSE 0 END) AS dev6_ok,
                   SUM(CASE WHEN dev_no = 6 THEN amt_err ELSE 0 END) AS dev6_err,
                   SUM(CASE WHEN dev_no = 7 THEN amt_ok ELSE 0 END) AS dev7_ok,
                   SUM(CASE WHEN dev_no = 7 THEN amt_err ELSE 0 END) AS dev7_err,
                   SUM(CASE WHEN dev_no = 8 THEN amt_ok ELSE 0 END) AS dev8_ok,
                   SUM(CASE WHEN dev_no = 8 THEN amt_err ELSE 0 END) AS dev8_err
            FROM mon_04_v
            WHERE ymd BETWEEN ? AND ?
            GROUP BY jepum_nm
            ORDER BY jepum_nm;
        """
        
        params = (from_dt, to_dt)
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()

        result_data = []
        for row in rows:
            product_data = {"jepum_nm": row[0]}
            for i in range(1, 9):
                ok_val = int(row[2*i - 1] or 0)
                err_val = int(row[2*i] or 0)
                
                # [수정] if 문을 제거하고 항상 객체 형태로 값을 할당합니다.
                product_data[f"{i}"] = {"ok": ok_val, "err": err_val}
            
            result_data.append(product_data)
            
        return jsonify(result_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# 기술통계분석용 (mon_07_v 뷰 사용)
@data_select_bp.route("/descriptive", methods=["GET"])
def get_daily_production_summary():
    """
    지정된 기간 동안의 일별 생산 데이터를 mon_07_v 뷰에서 조회합니다.
    (날짜, 양품, 불량 데이터를 반환)
    """
    # 1. 프론트엔드로부터 파라미터 받기
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")
    to_dt = request.args.get("to_dt")

    if not all([v_db, from_dt, to_dt]):
        return jsonify({"error": "모든 파라미터(v_db, from_dt, to_dt)가 필요합니다."}), 400

    try:
        # 2. DB 연결
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결에 실패했습니다: {v_db}"}), 500

        cur = conn.cursor()

        # 3. mon_07_v 뷰에서 데이터 조회 쿼리
        # ymd로 정렬하여 순서대로 데이터를 가져옵니다.
        query = """
            SELECT
                ymd,
                ok,
                err
            FROM
                mon_07_v
            WHERE
                ymd BETWEEN ? AND ?
            ORDER BY
                ymd;
        """
        
        params = (from_dt, to_dt)
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()

        # 4. 결과 데이터를 JSON 형식으로 가공
        result_data = []
        for row in rows:
            result_data.append({
                "ymd": row[0],
                "ok": int(row[1] or 0),
                "err": int(row[2] or 0)
            })
        
        return jsonify(result_data), 200

    except Exception as e:
        # 5. 에러 처리
        print(f"Error occurred in /daily-production-summary: {e}")
        return jsonify({"error": f"데이터 조회 중 오류 발생: {str(e)}"}), 500
    
@report_select_bp.route("/equipment-list", methods=["GET"])
def report_select_equipment_list():
    """
    안전관리 앱에서 사용하는 EQUIPMENT_LIST 테이블의 모든 장비를 조회합니다.
    호출 예시: /api/select/equip/list?v_db=16_UR
    """
    v_db = request.args.get("v_db")

    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500
        
        cur = conn.cursor()
        
        # EQUIPMENT_LIST 테이블에서 모든 데이터를 조회합니다.
        query = """
            SELECT code, name, location, category
            FROM EQUIPMENT_LIST
            ORDER BY code
        """
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()
        
        data = []
        for row in rows:
            data.append({
                "code": row[0],
                "name": row[1],
                "location": row[2],
                "category": row[3]
            })
            
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@report_select_bp.route("/questions", methods=["GET"])
def report_select_questions():
    """
    특정 장비 코드(code)에 해당하는 질문 목록을 조회합니다.
    호출 예시: /api/select/equip/questions?v_db=65_GW&code=GN1
    """
    v_db = request.args.get("v_db")
    equip_code = request.args.get("code") # 장비 코드를 파라미터로 받음

    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400
    if not equip_code:
        return jsonify({"error": "code 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500
        
        cur = conn.cursor()
        
        # QUESTION_LIST 테이블에서 code가 일치하는 질문들을 조회합니다.
        query = """
            SELECT question
            FROM QUESTION_LIST
            WHERE code = ?
        """
        cur.execute(query, (equip_code,)) # 파라미터 바인딩으로 SQL Injection 방지
        rows = cur.fetchall()
        conn.close()
        
        # 조회된 질문들을 하나의 리스트로 만듭니다.
        # 예: ['노후된 부품은 없는가?', '기구 안전 상태는 양호한가?', ...]
        question_list = [row[0] for row in rows]
        
        return json.dumps(question_list, cls=DecimalEncoder, ensure_ascii=False), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@report_select_bp.route("/history-log", methods=["GET"])
def report_select_history_log():
    """
    특정 장비 코드(code)에 해당하는 이력 로그를 조회합니다.
    호출 예시: /api/select/equip/history-log?v_db=65_GW&code=GN1
    """
    v_db = request.args.get("v_db")

    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500

        cur = conn.cursor()

        # HISTORY_LOG 테이블에서 code가 일치하는 이력 로그를 조회합니다.
        query = """
            SELECT code, question, answer, photo_filename
            FROM INSPECTION_MST
        """
        cur.execute(query) # 파라미터 바인딩으로 SQL Injection 방지
        rows = cur.fetchall()
        conn.close()

        # 조회된 이력 로그를 하나의 리스트로 만듭니다.
        history_log = [{"code": row[0], "question": row[1], "answer": row[2], "photo_filename": row[3]} for row in rows]

        return json.dumps(history_log, cls=DecimalEncoder, ensure_ascii=False), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# 파일 다운로드를 위한 최종 경로 설정 (안정적인 방식)
# 현재 파일(select.py)의 위치를 기준으로 프로젝트 루트를 찾고 'uploads' 폴더의 절대 경로를 생성합니다.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UPLOAD_FOLDER_ABSOLUTE_PATH = os.path.join(project_root, 'uploads')

@file_select_bp.route("/download/<path:filename>", methods=["GET"])
def download_file(filename):
    """
    서버의 uploads 폴더에 저장된 파일을 클라이언트에게 제공합니다.
    """
    try:
        # send_from_directory 함수를 사용하여 안전하게 파일을 전송합니다.
        return send_from_directory(
            directory=UPLOAD_FOLDER_ABSOLUTE_PATH,
            filename=filename
        )
    except FileNotFoundError:
        # 파일이 해당 경로에 존재하지 않을 경우 404 에러를 반환합니다.
        return jsonify({"error": f"File '{filename}' not found on server."}), 404
    except Exception as e:
        return jsonify({"error": "An internal server error occurred while processing the file."}), 500