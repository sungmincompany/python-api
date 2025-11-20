# main.py
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

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

# 1️⃣ [추가] 새로 만든 파일 불러오기 (경로 주의)
from routes.segsan import segsan_bp
from routes.common import common_bp  # 👈 추가!
from routes.stock import stock_bp

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

        # ✅ [여기 추가!] 주제별(Domain) 라우트
        # 이제 '/api/segsan'으로 시작하는 모든 요청은 segsan.py가 처리합니다.
        (segsan_bp, "/api/segsan"),
        (common_bp, "/api/common"), # 공통(기준정보) 관련 👈 추가!
        (stock_bp,  "/api/stock"), # 재고관리 관련 👈 추가!
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

    return app

if __name__ == "__main__":
    app = create_app()
    # 실제 운영 환경에서는 Gunicorn, uWSGI 같은 WSGI 서버를 사용하는 것을 권장합니다.
    app.run(debug=True, port=8999, host="0.0.0.0")
