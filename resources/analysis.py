# analysis_bp.py (최종 수정본)
# 1. 모든 .encode() / .decode() 제거
# 2. 모든 파라미터 마커를 '?'로 통일
# 3. 1차/2차 분석 쿼리를 안정적인 PIVOT 쿼리로 교체

from flask import Blueprint, request, jsonify, Response
from db import get_db_connection, DecimalEncoder
import json
import pandas as pd
import numpy as np
import decimal # Decimal 타입을 처리하기 위해 추가
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures # 2차항 생성을 위해 추가
from sklearn.pipeline import make_pipeline # 파이프라인 구성을 위해 추가

# 분석 관련 블루프린트
analysis_bp = Blueprint("analysis_bp", __name__)

# [수정] 기존 NumpyEncoder에 Decimal 처리 기능만 추가합니다.
class NumpyEncoder(json.JSONEncoder):
    """ Numpy 및 Decimal 타입 객체를 JSON으로 변환할 수 있도록 처리하는 클래스 """
    def default(self, obj):
        import numpy as np
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        # 이 한 줄만 추가하면 Decimal 오류가 해결됩니다.
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(NumpyEncoder, self).default(obj)

@analysis_bp.route("/xy-options", methods=["GET"])
def get_xy_options():
    """
    (수정) X축과 Y축으로 선택 가능한 항목 목록을 반환합니다.
    'factor_type' 파라미터에 따라 반환되는 X축 옵션이 달라집니다.
    - factor_type='x1': 1차 요소(x1)와 y 요소 반환
    - factor_type='x2': 2차 요소(x2)와 y 요소 반환
    - 미지정: 모든 x(x1, x2) 요소와 y 요소 반환
    """
    v_db = request.args.get("v_db")
    # 'x1', 'x2'를 받을 파라미터 추가
    gbn_type = request.args.get("gbn_type") 

    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # factor_type 값에 따라 WHERE 절을 동적으로 구성
        if gbn_type == 'x1':
            # 1차 분석: gbn이 'x1' 또는 'y'인 항목 조회
            where_clause = "WHERE gbn IN ('x1', 'y')"
        elif gbn_type == 'x2':
            # 2차 분석: gbn이 'x2' 또는 'y'인 항목 조회
            where_clause = "WHERE gbn IN ('x2', 'y')"
        else:
            # 파라미터가 없는 경우: gbn('x', 'y') 항목 조회
            where_clause = "WHERE gbn IN ('x', 'y')"

        # 동적으로 생성된 WHERE 절을 포함하여 쿼리 실행
        query = f"""
            SELECT gbn, col_1 
            FROM mon_06_v 
            {where_clause}
            GROUP BY gbn, col_1 
            ORDER BY gbn, col_1;
        """
        cur.execute(query) # 이 쿼리는 파라미터가 없으므로 그대로 둡니다.
        rows = cur.fetchall()
        conn.close()

        options = {
            "x_options": [],
            "y_options": []
        }
        for row in rows:
            gbn, col_1 = row
            # gbn이 'x'로 시작하면 x_options에 추가 ('x1', 'x2' 모두 해당)
            if gbn.startswith('x'):
                options["x_options"].append(col_1)
            elif gbn == 'y':
                options["y_options"].append(col_1)
        
        # 한글 깨짐 방지를 위해 json.dumps와 Response 사용
        json_response = json.dumps(options, ensure_ascii=False)
        return Response(json_response, mimetype='application/json')

    except Exception as e:
        # 오류 발생 시 한글 오류 메시지 반환
        error_response = json.dumps({"error": str(e)}, ensure_ascii=False)
        return Response(error_response, status=500, mimetype='application/json')

# 상관분석 X축, Y축 선택
# [최종] /dynamic-analysis (JOIN ON ymd 쿼리로 교체)
# [최종 디버깅] /dynamic-analysis (데이터 직접 확인)
# [최종] /dynamic-analysis (Row -> Tuple 강제 변환)
# [최종] /dynamic-analysis (SQL 분리 + Sklearn 경고 수정)
@analysis_bp.route("/dynamic-analysis", methods=["GET"])
def dynamic_analysis():
    """
    (최종 수정) 
    1. pyodbc.Row -> tuple 강제 변환 (완료)
    2. INSERT와 SELECT SCOPE_IDENTITY() 쿼리를 분리 실행
    3. Sklearn predict 경고 해결 (DataFrame 전달)
    """
    
    # --- 1. 파라미터 받기 (인코딩 제거) ---
    v_db = request.args.get("v_db")
    gbn_x = request.args.get("gbn_x")
    col_1_x = request.args.get("col_1_x") 
    gbn_y = request.args.get("gbn_y")
    col_1_y = request.args.get("col_1_y") 
    from_dt = request.args.get("from_dt")
    to_dt = request.args.get("to_dt")

    if not all([v_db, gbn_x, col_1_x, gbn_y, col_1_y, from_dt, to_dt]):
        return jsonify({"error": "모든 파라미터가 필요합니다."}), 400

    try:
        # --- 2. DB 연결 및 데이터 조회 (JOIN ON ymd 쿼리) ---
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        query = """
            SELECT 
                x_data.amt AS x_amt,
                y_data.amt AS y_amt
            FROM 
            (
                SELECT ymd, amt
                FROM mon_06_v
                WHERE gbn = ? AND col_1 = ? AND ymd BETWEEN ? AND ?
            ) AS x_data
            JOIN 
            (
                SELECT ymd, amt
                FROM mon_06_v
                WHERE gbn = ? AND col_1 = ? AND ymd BETWEEN ? AND ?
            ) AS y_data ON x_data.ymd = y_data.ymd;
        """
        params = (
            gbn_x, col_1_x, from_dt, to_dt,
            gbn_y, col_1_y, from_dt, to_dt 
        )
        
        cur.execute(query, params)
        rows = cur.fetchall() 
        
        # [수정 1. pyodbc.Row -> tuple 강제 변환]
        rows_as_tuples = [tuple(row) for row in rows]
        column_list = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows_as_tuples, columns=column_list) 
        
        df['x_amt'] = pd.to_numeric(df['x_amt'], errors='coerce')
        df['y_amt'] = pd.to_numeric(df['y_amt'], errors='coerce')
        df.dropna(inplace=True)
        
        if len(df) < 2:
            conn.close()
            error_msg = {"error": "분석에 필요한 데이터 쌍(pair)이 부족합니다."}
            return Response(json.dumps(error_msg, ensure_ascii=False), status=400, mimetype='application/json')

        # --- 3. 분석 수행 ---
        correlation = df['x_amt'].corr(df['y_amt'])
        X = df[['x_amt']] # 'x_amt' 컬럼명을 feature 이름으로 사용
        y = df['y_amt']
        model = LinearRegression()
        model.fit(X, y)
        coefficient = model.coef_[0]
        intercept = model.intercept_
        r_squared = model.score(X, y)

        # --- 4. 데이터 조립 ---
        scatter_points = df[['x_amt', 'y_amt']].values.tolist()
        min_x = df['x_amt'].min()
        max_x = df['x_amt'].max()

        # [수정 2. Sklearn 경고 해결]
        # predict에도 fit과 동일한 feature 이름('x_amt')을 가진 DataFrame을 전달
        x_pred_input = pd.DataFrame({'x_amt': [min_x, max_x]})
        y_pred_output = model.predict(x_pred_input)
        
        line_points_data = [[
            { 'coord': [min_x, y_pred_output[0]] },
            { 'coord': [max_x, y_pred_output[1]] }
        ]]
        
        x_variable_str = f"{gbn_x} / {col_1_x}"
        y_variable_str = f"{gbn_y} / {col_1_y}"
        equation_str = f"y = {coefficient:.4f}x + {intercept:.4f}"
        interpretation_str = f"'{col_1_x}' 값이 1단위 증가할 때 '{col_1_y}' 값은(는) 평균적으로 {coefficient:.4f}만큼 변합니다. 이 모델은 Y값 변동성의 {r_squared:.2%}를 설명합니다."

        # --- [수정 3. DB 저장 쿼리 분리] ---
        insert_query = """
            INSERT INTO analysis_history 
            (analyst_nm, x_variable, y_variable, correlation, r_squared, equation, interpretation, scatter_data, line_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        # (SCOPE_IDENTITY() 제거)
        
        line_points_data_for_db = [[min_x, y_pred_output[0]], [max_x, y_pred_output[1]]]
        
        insert_params = (
            '관리자', x_variable_str, y_variable_str, correlation, r_squared, 
            equation_str, interpretation_str,
            json.dumps(scatter_points, cls=NumpyEncoder), 
            json.dumps(line_points_data_for_db, cls=NumpyEncoder)
        )
        
        # 1. INSERT 실행
        cur.execute(insert_query, insert_params)
        
        # 2. 새로 생성된 ID 조회 (별도 쿼리로 실행)
        cur.execute("SELECT CAST(SCOPE_IDENTITY() AS INT);")
        new_analysis_id = cur.fetchone()[0]
        
        # 3. 트랜잭션 커밋
        conn.commit()
        conn.close()
        # --- [수정 끝] ---

        # 5. 응답
        response_data = {
            "new_analysis_id": new_analysis_id,
            "analysis_summary": {
                "x_variable": x_variable_str, "y_variable": y_variable_str,
                "correlation": correlation, "r_squared": r_squared,
                "equation": equation_str, "interpretation": interpretation_str
            },
            "scatter_data": scatter_points, "line_data": line_points_data
        }
        
        json_response = json.dumps(response_data, cls=NumpyEncoder, ensure_ascii=False)
        return Response(json_response, mimetype='application/json')

    except Exception as e:
        print(f"Error occurred in /dynamic-analysis: {e}") 
        error_response = json.dumps({"error": f"분석 중 오류 발생: {str(e)}"}, ensure_ascii=False)
        return Response(error_response, status=500, mimetype='application/json')
    
@analysis_bp.route("/collect-data", methods=["GET"])
def collect_data():
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")
    to_dt = request.args.get("to_dt")
    if not v_db:
        return jsonify({"error": "v_db 파라미터가 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # [수정] 파라미터 마커 '?'로 변경
        query = """
            SELECT ymd, col_1, amt
            FROM mon_06_v 
            WHERE ymd BETWEEN ? AND ?
        """
        cur.execute(query, (from_dt, to_dt))
        rows = cur.fetchall()
        conn.close()

        data = []
        for row in rows:
            data.append({
                "ymd": row[0],
                "col_1": row[1],
                "amt": row[2]
            })
        return json.dumps(data, cls=DecimalEncoder, ensure_ascii=False), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# [최종] /dynamic-analysis-2nd (모든 수정사항 적용)
@analysis_bp.route("/dynamic-analysis-2nd", methods=["GET"])
def dynamic_analysis_2nd():
    """
    (최종 수정) /dynamic-analysis와 동일하게 모든 수정사항 적용
    1. .encode() / .decode() 모두 제거
    2. 'ymd' 기준 'JOIN ON ymd' 쿼리 사용
    3. DB 파라미터 마커 '?' 사용
    4. pyodbc.Row -> tuple(row) 강제 변환
    5. Sklearn predict 경고 해결 (DataFrame 전달)
    """
    
    # --- 1. 파라미터 받기 (인코딩 제거) ---
    v_db = request.args.get("v_db")
    gbn_x = request.args.get("gbn_x")
    col_1_x = request.args.get("col_1_x") # .encode() 제거
    gbn_y = request.args.get("gbn_y")
    col_1_y = request.args.get("col_1_y") # .encode() 제거
    from_dt = request.args.get("from_dt")
    to_dt = request.args.get("to_dt")

    if not all([v_db, gbn_x, col_1_x, gbn_y, col_1_y, from_dt, to_dt]):
        return jsonify({"error": "모든 파라미터가 필요합니다."}), 400

    try:
        # --- 2. DB 연결 및 데이터 조회 (쿼리 로직 전면 수정) ---
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # [수정 1. 'JOIN ON ymd' 쿼리 및 '?' 마커]
        query = """
            SELECT 
                x_data.amt AS x_amt,
                y_data.amt AS y_amt
            FROM 
            (
                SELECT ymd, amt
                FROM mon_06_v
                WHERE gbn = ? AND col_1 = ? AND ymd BETWEEN ? AND ?
            ) AS x_data
            JOIN 
            (
                SELECT ymd, amt
                FROM mon_06_v
                WHERE gbn = ? AND col_1 = ? AND ymd BETWEEN ? AND ?
            ) AS y_data ON x_data.ymd = y_data.ymd;
        """
        
        # [수정 2. 새 JOIN 쿼리에 맞는 파라미터]
        params = (
            gbn_x, col_1_x, from_dt, to_dt,  # x_data 서브쿼리용
            gbn_y, col_1_y, from_dt, to_dt   # y_data 서브쿼리용
        )
        
        cur.execute(query, params)
        rows = cur.fetchall() # list[pyodbc.Row] 반환

        # [수정 3. pyodbc.Row -> tuple 강제 변환]
        rows_as_tuples = [tuple(row) for row in rows]
        column_names = [desc[0] for desc in cur.description]
        
        conn.close() # DB 연결은 여기서 닫아도 됩니다.

        # [수정 4. 변환된 'rows_as_tuples' 사용]
        df = pd.DataFrame(rows_as_tuples, columns=column_names)
        
        df['x_amt'] = pd.to_numeric(df['x_amt'], errors='coerce')
        df['y_amt'] = pd.to_numeric(df['y_amt'], errors='coerce')
        df.dropna(inplace=True)

        if len(df) < 3: # 2차 분석은 최소 3개의 데이터 필요
            error_msg = {"error": "분석에 필요한 데이터 쌍(pair)이 부족합니다."}
            return Response(json.dumps(error_msg, ensure_ascii=False), status=400, mimetype='application/json')

        # --- 3. 분석 수행 (2차 비선형 회귀) ---
        X = df[['x_amt']] # 'x_amt' 컬럼명을 feature 이름으로 사용
        y = df['y_amt']

        degree = 2
        # (include_bias=False는 a, b 계수만 추출하기 위함. c(절편)는 LinearRegression이 찾음)
        poly_model = make_pipeline(PolynomialFeatures(degree, include_bias=False), LinearRegression())
        poly_model.fit(X, y)

        coefs = poly_model.named_steps['linearregression'].coef_
        intercept = poly_model.named_steps['linearregression'].intercept_
        
        # 계수가 2개(x, x^2)인지 확인 (데이터가 너무 적으면 1개만 나올 수 있음)
        if len(coefs) < 2:
             # 안전 장치: 데이터가 부족하면 1차 회귀로 처리 (혹은 오류 반환)
             coef_x1 = coefs[0]
             coef_x2 = 0.0
        else:
             coef_x1 = coefs[0] # b (x의 계수)
             coef_x2 = coefs[1] # a (x^2의 계수)

        r_squared = poly_model.score(X, y)

        # --- 4. 프론트엔드 데이터 조립 (곡선) ---
        scatter_points = df[['x_amt', 'y_amt']].values.tolist()
        min_x = df['x_amt'].min()
        max_x = df['x_amt'].max()

        # [수정 5. Sklearn 경고 해결]
        # predict에도 fit과 동일한 feature 이름('x_amt')을 가진 DataFrame을 전달
        x_range_input = pd.DataFrame({'x_amt': np.linspace(min_x, max_x, 100)})
        y_pred_range = poly_model.predict(x_range_input)
        
        # [[x, y], ...] 형식으로 변환
        curve_points = [[x_range_input.iloc[i, 0], y_pred_range[i]] for i in range(len(x_range_input))]

        # [수정 6. .decode() 제거]
        x_variable_str = f"{gbn_x} / {col_1_x}"
        y_variable_str = f"{gbn_y} / {col_1_y}"
        equation_str = f"y = {coef_x2:.4f}x² + {coef_x1:.4f}x + {intercept:.4f}"
        interpretation_str = f"'{col_1_x}'와(과) '{col_1_y}'는 2차 곡선 관계를 보입니다. 이 모델은 Y값 변동성의 {r_squared:.2%}를 설명합니다."
        
        response_data = {
            "analysis_summary": {
                "x_variable": x_variable_str,
                "y_variable": y_variable_str,
                "correlation": df['x_amt'].corr(df['y_amt']), 
                "r_squared": r_squared,
                "equation": equation_str,
                "interpretation": interpretation_str
            },
            "scatter_data": scatter_points,
            "line_data": curve_points # 곡선 데이터
        }
        
        # --- 5. 최종 JSON 응답 생성 (DB 저장 로직 없음) ---
        json_response = json.dumps(response_data, cls=NumpyEncoder, ensure_ascii=False)
        return Response(json_response, mimetype='application/json')

    except Exception as e:
        print(f"Error occurred in /dynamic-analysis-2nd-order: {e}")
        error_response = json.dumps({"error": f"분석 중 오류 발생: {str(e)}"}, ensure_ascii=False)
        return Response(error_response, status=500, mimetype='application/json')
    
# --- 1. 분석 이력 목록 조회 API ---
@analysis_bp.route("/history", methods=["GET"])
def get_analysis_history():
    """
    (최종 수정) 파라미터 마커 '?'로 변경
    """
    v_db = request.args.get("v_db")
    from_dt = request.args.get("from_dt")
    to_dt = request.args.get("to_dt")

    if not all([v_db, from_dt, to_dt]):
        return jsonify({"error": "v_db, from_dt, to_dt 파라미터가 모두 필요합니다."}), 400
    
    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()
        
        # [수정] 파라미터 마커 '?'로 변경
        query = """
            SELECT id, analysis_dt, analyst_nm, x_variable, y_variable, correlation, r_squared 
            FROM analysis_history 
            WHERE CONVERT(VARCHAR, analysis_dt, 112) BETWEEN ? AND ?
            ORDER BY id DESC;
        """
        cur.execute(query, (from_dt, to_dt))
        
        columns = [column[0] for column in cur.description]
        history_list = []
        for row in cur.fetchall():
            row_dict = dict(zip(columns, row))
            if 'analysis_dt' in row_dict and row_dict['analysis_dt']:
                # [수정] ? -> :S 로 변경 (포맷팅 오류 수정)
                row_dict['analysis_dt'] = row_dict['analysis_dt'].strftime('%Y-%m-%d %H:%M:%S')
            history_list.append(row_dict)
            
        conn.close()

        return Response(json.dumps(history_list, cls=NumpyEncoder, ensure_ascii=False), mimetype='application/json')

    except Exception as e:
        print(f"Error occurred in /history: {e}")
        return Response(json.dumps({"error": f"오류 발생: {str(e)}"}, ensure_ascii=False), status=500, mimetype='application/json')

# --- 2. 분석 결과 상세 조회 API ---
@analysis_bp.route("/result-report", methods=["GET"])
def get_analysis_result_report():
    """
    (최종 수정) 파라미터 마커 '?'로 변경
    """
    v_db = request.args.get("v_db")
    analysis_id = request.args.get("analysis_id")

    if not all([v_db, analysis_id]):
        return jsonify({"error": "v_db와 analysis_id 파라미터가 모두 필요합니다."}), 400

    try:
        conn = get_db_connection(v_db)
        cur = conn.cursor()

        # [수정] 파라미터 마커 '?'로 변경
        query = "SELECT * FROM analysis_history WHERE id = ?"
        cur.execute(query, (analysis_id,))
        
        db_row = cur.fetchone()
        if not db_row:
            conn.close()
            return jsonify({"error": "해당 ID의 분석 결과를 찾을 수 없습니다."}), 404
        
        columns = [column[0] for column in cur.description]
        result = dict(zip(columns, db_row))
        conn.close()
        
        # [수정] 불필요한 대괄호를 제거하여 [{coord}, {coord}] 형식으로 변경합니다.
        line_data_from_db = json.loads(result['line_data'])
        formatted_line_data = [
            { 'coord': line_data_from_db[0] },
            { 'coord': line_data_from_db[1] }
        ]

        report_data = {
            "analysis_summary": {
                # [수정] ? -> :S 로 변경 (포맷팅 오류 수정)
                "report_date": result['analysis_dt'].strftime('%Y-%m-%d %H:%M:%S'),
                "analyst": result['analyst_nm'],
                "x_variable": result['x_variable'],
                "y_variable": result['y_variable'],
                "correlation": result['correlation'],
                "r_squared": result['r_squared'],
                "equation": result['equation'],
                "interpretation": result['interpretation']
            },
            "scatter_data": json.loads(result['scatter_data']),
            "line_data": formatted_line_data # 수정된 데이터를 사용
        }

        return Response(json.dumps(report_data, cls=NumpyEncoder, ensure_ascii=False), mimetype='application/json')

    except Exception as e:
        print(f"Error occurred in /report for id {analysis_id}: {e}")
        return Response(json.dumps({"error": f"오류 발생: {str(e)}"}, ensure_ascii=False), status=500, mimetype='application/json')