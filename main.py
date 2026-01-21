# main.py
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import datetime
import pyodbc
from db import get_db_connection

# 기능별 블루프린트 임포트
from resources.select import (
    vender_select_bp, jepum_select_bp, stock_select_bp, suju_select_bp,
    equip_select_bp, segsan_select_bp, etc_select_bp, smart_select_bp,
    data_select_bp, report_select_bp, file_select_bp
)
from resources.insert import (
    suju_insert_bp, stock_insert_bp, etc_insert_bp, file_insert_bp,
    report_insert_bp
)
from resources.update import (
    suju_update_bp, stock_update_bp, etc_update_bp
)
from resources.delete import (
    suju_delete_bp, stock_delete_bp, etc_delete_bp
)
from resources.analysis import analysis_bp

from routes.segsan import segsan_bp
from routes.common import common_bp
from routes.stock import stock_bp
from routes.chulha import chulha_bp
from routes.data import data_bp

# [추가] 97gm 전용 라우트 임포트
from routes.custom_97gm import bp_97gm

# -----------------------------------------------------------
# [추가] API 경로 -> 한글 프로그램명 매핑
# -----------------------------------------------------------
API_NAME_MAP = {
    # [생산 관련]
    "/api/segsan/insert": "생산실적 등록",
    "/api/segsan/list": "생산실적 조회",
    "/api/segsan/update": "생산실적 수정",
    "/api/segsan/delete": "생산실적 삭제",
    "/api/select/segsan/process": "공정별 실적 조회",

    # [공통/기준정보]
    "/api/common/jepum": "제품 기준정보 조회",
    "/api/common/vender": "거래처 기준정보 조회",
    "/api/select/vender/all": "거래처 전체 조회",
    "/api/select/vender/out": "매출처 조회",
    "/api/select/jepum/all": "제품 전체 조회",
    "/api/select/jepum/jepum": "완제품 조회",

    # [재고/출하]
    "/api/stock/list": "재고 현황 조회",
    "/api/select/stock/jepum": "제품별 재고 조회",
    "/api/select/stock/jepum-out": "완제품 출고 조회",
    "/api/chulha/insert": "출하 등록",
    "/api/chulha/list": "출하 조회",
    "/api/chulha/update": "출하 수정",
    "/api/chulha/delete": "출하 삭제",
    "/api/insert/stock/out": "출고 등록 (구버전)",
    "/api/update/stock/update": "출고 수정 (구버전)",
    "/api/delete/stock/delete": "출고 삭제 (구버전)",

    # [주문]
    "/api/select/suju/all": "주문 조회",
    "/api/insert/suju/register": "주문 등록",
    "/api/update/suju/update": "주문 수정",
    "/api/delete/suju/delete": "주문 삭제",

    # [데이터/설비]
    "/api/data/measurement": "측정 데이터 조회",
    "/api/select/smart/smart-log": "설비 데이터 수집 로그",
    "/api/select/data/smart-prg-cd": "Smart 공정 목록",
    "/api/select/data/equip-down-time": "기기별 비가동 시간",
    "/api/select/data/process-equip": "공정-기기별 가동시간",
    "/api/select/data/jepum-defect-rate": "제품별 불량률",
    "/api/select/data/line-defect-rate": "라인별 불량률",
    "/api/select/data/descriptive": "기술통계 분석",
    "/api/select/equip/mst": "설비 목록 조회",
    "/api/select/equip/inspect": "설비 점검내역 조회",

    # [기타/분석]
    "/api/analysis/xy-options": "분석 항목 조회",
    "/api/analysis/dynamic-analysis": "다이나믹 상관분석",
    "/api/analysis/collect-data": "분석용 데이터 수집",
    "/api/analysis/history": "분석 이력 조회",
    "/api/analysis/result-report": "분석 결과 리포트",
    
    # [ETC/테스트]
    "/api/select/etc/test-result": "TEST 공정 결과 조회",
    "/api/insert/etc/test-result": "TEST 실적 등록",
    "/api/update/etc/test-result": "TEST 실적 수정",
    "/api/delete/etc/test-result": "TEST 실적 삭제",
    "/api/select/etc/lot_no_inform": "LOT NO 정보 조회",
}

def create_app():
    """
    Flask 애플리케이션 팩토리 함수.
    앱 인스턴스를 생성하고, 각종 설정을 마친 후 반환합니다.
    """
    app = Flask(__name__)
    CORS(app, resources={
        r"/api/.*": {"origins": "*"}
    })

    # 등록할 블루프린트들을 리스트로 관리하여 가독성과 유지보수성을 높입니다.
    # 각 항목은 (블루프린트 객체, URL 접두사) 형태의 튜플입니다.
    blueprints_to_register = [
        # --- Select ---
        (vender_select_bp, "/api/select/vender"),
        (jepum_select_bp, "/api/select/jepum"),
        (stock_select_bp, "/api/select/stock"),
        (suju_select_bp, "/api/select/suju"),
        (equip_select_bp, "/api/select/equip"),
        (segsan_select_bp, "/api/select/segsan"),
        (etc_select_bp, "/api/select/etc"),
        (smart_select_bp, "/api/select/smart"),
        (data_select_bp, "/api/select/data"),
        (report_select_bp, "/api/select/report"),
        (file_select_bp, "/api/select/file"),

        # --- Insert ---
        (suju_insert_bp, "/api/insert/suju"),
        (stock_insert_bp, "/api/insert/stock"),
        (etc_insert_bp, "/api/insert/etc"),
        (file_insert_bp, "/api/insert/file"),
        (report_insert_bp, "/api/insert/report"),

        # --- Update ---
        (suju_update_bp, "/api/update/suju"),
        (stock_update_bp, "/api/update/stock"),
        (etc_update_bp, "/api/update/etc"),

        # --- Delete ---
        (suju_delete_bp, "/api/delete/suju"),
        (stock_delete_bp, "/api/delete/stock"),
        (etc_delete_bp, "/api/delete/etc"),

        # --- Analysis ---
        (analysis_bp, "/api/analysis"),

        # 주제별(Domain) 라우트
        (segsan_bp, "/api/segsan"),
        (common_bp, "/api/common"), 
        (stock_bp,  "/api/stock"), 
        (chulha_bp, "/api/chulha"), 
        (data_bp,   "/api/data"),   

        # [추가] 97gm 전용 블루프린트 등록
        (bp_97gm,   "/api/97gm"),
    ]

    # 루프를 통해 리스트에 있는 모든 블루프린트를 자동으로 등록합니다.
    for bp, url_prefix in blueprints_to_register:
        app.register_blueprint(bp, url_prefix=url_prefix)

    @app.route("/")
    def index():
        """
        API의 루트 엔드포인트.
        서버가 정상적으로 실행 중인지 확인하는 용도로 사용됩니다. (Health Check)
        """
        return jsonify({
            "status": "ok",
            "message": "Main API is running successfully."
        })

    # ==================================================================
    # [수정됨] 모든 요청 처리 후 실행되는 로그 기록 훅 (After Request Hook)
    # ==================================================================
    @app.after_request
    def log_request_to_db(response):
        # 1. v_db 파라미터 확인 (없으면 로그 기록 불가하므로 패스)
        v_db = request.args.get('v_db')
        if not v_db:
            return response

        # 2. 로깅 제외 경로 설정
        if request.path == '/' or request.path.startswith('/static'):
            return response

        conn = None
        try:
            conn = get_db_connection(v_db)
            if not conn:
                return response
            
            cur = conn.cursor()

            # -------------------------------------------------------
            # 1. IP 주소 처리 (External IP 우선)
            # -------------------------------------------------------
            # X-Forwarded-For 헤더가 있으면 그 중 첫 번째 IP가 실제 클라이언트 IP입니다.
            # 헤더가 없으면 remote_addr(직접 연결된 IP)를 사용합니다.
            if request.headers.getlist("X-Forwarded-For"):
                ip_addr = request.headers.getlist("X-Forwarded-For")[0]
            else:
                ip_addr = request.remote_addr

            # -------------------------------------------------------
            # 2. 사용 프로그램(used_pgm) 한글 변환
            # -------------------------------------------------------
            # API_NAME_MAP은 main.py 상단에 정의되어 있다고 가정합니다.
            # 매핑된 한글명이 있으면 사용, 없으면 영문 경로 그대로 사용
            current_path = request.path
            used_pgm = API_NAME_MAP.get(current_path, current_path)
            
            # DB 컬럼 길이에 맞춰 자르기 (혹시 모를 에러 방지)
            if len(used_pgm) > 1000: used_pgm = used_pgm[:1000]

            # -------------------------------------------------------
            # 3. 고정값 설정 (요청하신 내용 반영)
            # -------------------------------------------------------
            emp_no = 'K0000999'   # 사번
            user_id = 'mobile'    # 아이디
            org_cd = 'K'          # 조직코드
            dept_cd = 'D0400'     # 부서코드
            emp_nmk = '모바일'     # 성명
            used_cnt = 0          # 사용횟수
            used_cnt3 = 0         # 추가필드

            # -------------------------------------------------------
            # 4. INSERT 실행 (out_time, tot_time 제외)
            # -------------------------------------------------------
            # MSSQL 2008에서도 ? 파라미터가 자동으로 특수문자를 처리해줍니다.
            sql = """
                INSERT INTO bmtlogh (
                    emp_no, in_time, user_id, org_cd, dept_cd, 
                    used_pgm, used_cnt, 
                    emp_nmk, ip_addr, used_cnt3
                ) VALUES (
                    ?, GETDATE(), ?, ?, ?, 
                    ?, ?, 
                    ?, ?, ?
                )
            """
            
            # 파라미터 매핑
            cur.execute(sql, (
                emp_no,     # 1. K0000999
                user_id,    # 2. mobile
                org_cd,     # 3. K
                dept_cd,    # 4. D0400
                used_pgm,   # 5. 프로그램명 (슬래시 포함 가능)
                used_cnt,   # 6. 0
                emp_nmk,    # 7. 모바일
                ip_addr,    # 8. IP주소
                used_cnt3   # 9. 0
            ))
            conn.commit()

        except pyodbc.ProgrammingError as e:
            # SQL State 42S02: Base table or view not found
            # 테이블이 없어서 나는 에러는 조용히 무시 (로그만 찍음)
            if '42S02' in str(e):
                pass
            else:
                print(f"[Log Error] Programming Error: {e}")
        except Exception as e:
            # 그 외 에러 (연결 실패 등) 도 API 응답에는 지장을 주지 않도록 출력만 하고 넘어감
            print(f"[Log Error] Failed to insert log: {e}")
        finally:
            if conn:
                conn.close()

        return response

    return app

if __name__ == "__main__":
    app = create_app()
    # 실제 운영 환경에서는 Gunicorn, uWSGI 같은 WSGI 서버를 사용하는 것을 권장합니다.
    app.run(debug=True, port=8999, host="0.0.0.0")