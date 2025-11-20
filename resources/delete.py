# resources/suju_delete.py
from flask import Blueprint, jsonify, request
from db import get_db_connection

suju_delete_bp = Blueprint("suju_delete", __name__)
# 기존 suju_delete_bp와 별도로, stock_delete_bp 추가
stock_delete_bp = Blueprint("stock_delete_bp", __name__)
etc_delete_bp = Blueprint("etc_delete", __name__)

@suju_delete_bp.route("/delete", methods=["DELETE"])
def delete_suju():
    # 쿼리스트링으로 전달되는 v_db와 suju_cd 파라미터 필수
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    suju_cd = request.args.get("suju_cd")
    if not suju_cd:
        return jsonify({"error": "suju_cd parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        query = "DELETE FROM suju_mst WHERE suju_cd = ?"
        cur.execute(query, (suju_cd,))
        conn.commit()
        conn.close()
        return jsonify({"message": "DELETE success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@stock_delete_bp.route("/delete", methods=["DELETE"])
def delete_stock():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db parameter is required"}), 400

    inout_no = request.args.get("inout_no")
    if not inout_no:
        return jsonify({"error": "inout_no parameter is required"}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB connection failed for {v_db}"}), 500

        cur = conn.cursor()
        sql = "DELETE FROM stock_mst WHERE inout_no = ?"
        cur.execute(sql, (inout_no,))
        conn.commit()
        conn.close()
        return jsonify({"message": "출고 DELETE success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@etc_delete_bp.route("/test-result", methods=["DELETE"])
def delete_test_result():
    """
    DELETE /api/delete/test_result?v_db=16_UR&lot_no=T123
    """
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    lot_no = request.args.get("lot_no")
    if not lot_no:
        return jsonify({"error": "lot_no 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500

        cur = conn.cursor()
        query = """
            DELETE FROM lot_hst
            WHERE lot_no = ?
              AND prg_cd = '170'
        """
        cur.execute(query, (lot_no,))
        conn.commit()
        conn.close()

        return jsonify({"message": "TEST 실적 삭제 성공"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@etc_delete_bp.route("/tapping-result", methods=["DELETE"])
def delete_tapping():
    v_db = request.args.get("v_db")
    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    lot_no = request.args.get("lot_no")
    if not lot_no:
        return jsonify({"error": "lot_no 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        if not conn:
            return jsonify({"error": f"DB 연결 실패: {v_db}"}), 500

        cur = conn.cursor()
        # prg_cd=180인 해당 LOT 전부 삭제
        sql = """
            DELETE FROM lot_hst
            WHERE lot_no = ?
              AND prg_cd = '180'
        """
        cur.execute(sql, (lot_no,))
        conn.commit()
        conn.close()

        return jsonify({"message": "Taping 삭제 성공"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500