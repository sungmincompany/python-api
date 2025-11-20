# db.py (pyodbc 버전으로 수정한 코드)

import pyodbc  # 1. 'pymssql' 대신 'pyodbc'를 import
import json
from decimal import Decimal

# 2. ODBC 드라이버 이름 설정 (가장 중요!)
# PowerShell에서 'Get-OdbcDriver' 명령어로 확인한 이름을 넣으세요.
# 'MSSQL 2008'을 사용 중이시므로 'SQL Server Native Client 11.0' 또는 'SQL Server'를 권장합니다.
DRIVER_NAME = 'ODBC Driver 17 for SQL Server'
# 또는 'SQL Server' (가장 기본적인 드라이버)
# 또는 'ODBC Driver 17 for SQL Server' (최신 드라이버)

# 특별한 업체에 대해서만 별도 설정을 할 경우, override dictionary를 사용합니다.
# (기존 로직과 동일)
OVERRIDE_CONFIG = {
    "16_UR": { 
        "server": "agen81.iptime.org,5647",
        "user": "16-UR",
        "password": "16-UR",
        "database": "16_UR",
        "charset": "EUC-KR" # 이 값은 연결 후 인코딩 설정에 사용됩니다.
    },
    "97_GM": { 
        "server": "180.71.47.241,2488",
        "user": "97_GM",
        "password": "97_GM",
        "database": "97_GM",
        "charset": "EUC-KR"
    },
}

def get_db_connection(v_db):
    """
    v_db (문자열)를 입력받아,
    기본적으로 server/port는 고정이고, 
    user, password, database는 v_db로 설정하지만,
    OVERRIDE_CONFIG에 해당 업체가 있으면 해당 값을 사용합니다.
    
    (pyodbc 버전, setdecoding 오류 수정)
    """
    # OVERRIDE_CONFIG에 해당 업체가 있다면 override 설정 사용
    if v_db in OVERRIDE_CONFIG:
        config = OVERRIDE_CONFIG[v_db]
    else:
        # 기본 설정
        config = {
            "server": "agen072.iptime.org,2488",
            "user": v_db,
            "password": v_db,
            "database": v_db,
            "charset": "EUC-KR" # 기본 인코딩
        }
    
    conn_str = "" # 디버깅을 위해 try 바깥쪽에 선언
    try:
        # 서버 주소와 포트 분리
        server_full = config['server']
        
        if ':' in server_full:
            server_host, server_port = server_full.split(':')
        else:
            server_host = server_full
            server_port = '1433' # 기본 포트
        
        # 3. pyodbc 연결 문자열(Connection String) 생성
        conn_str = (
            f"DRIVER={{{DRIVER_NAME}}};"
            f"SERVER={server_host};"
            f"PORT={server_port};"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={config['password']};"
        )
        
        # 4. pyodbc.connect 사용
        conn = pyodbc.connect(conn_str, timeout=5)

        # 5. --- [수정된 인코딩 설정] ---
        # setdecoding은 SQL_CHAR, SQL_WCHAR 등 일부 타입에만 적용 가능합니다.
        db_charset = config["charset"]
        
        # DB -> Python (데이터 읽기: Decoding)
        # 오류를 일으킨 SQL_VARCHAR, SQL_WVARCHAR를 제거합니다.
        conn.setdecoding(pyodbc.SQL_CHAR, encoding=db_charset)
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding=db_charset)
        
        # Python -> DB (데이터 쓰기: Encoding)
        conn.setencoding(encoding=db_charset)
        # --- [수정 끝] ---

        return conn
        
    except pyodbc.Error as e: 
        print(f"DB Connection Error (vendor={v_db}): {e}")
        print(f"config={config}")
        print(f"ConnectionString={conn_str}")
        return None
    except Exception as e:
        # 'setdecoding' 오류 등 예상치 못한 오류
        print(f"An unexpected error occurred (vendor={v_db}): {e}")
        print(f"config={config}")
        return None


class DecimalEncoder(json.JSONEncoder):
    """
    MSSQL 쿼리 결과로 반환된 Decimal 객체를 float로 변환하여 JSON 직렬화할 때 사용.
    json.dumps(data, cls=DecimalEncoder) 형식으로 사용.
    
    (이 클래스는 pyodbc와 pymssql에 관계없이 100% 동일하게 작동합니다. 수정 불필요.)
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)