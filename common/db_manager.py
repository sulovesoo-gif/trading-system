import os
import psycopg2
from psycopg2.extras import RealDictCursor # 딕셔너리 형태로 결과 반환 (추천)
# from dotenv import load_dotenv
# 수정된 부분: 상대 경로 (.)를 추가하여 같은 폴더 내 config를 참조하게 합니다.
# from .config import get_env_path

# 공통 경로에서 .env 로드
# load_dotenv(get_env_path())

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        autocommit=True
    )

class DBHandler:
    """기존 코드에서 DBHandler 클래스가 필요하므로 여기에 정의합니다."""
    def __init__(self):
        self.conn = get_db_connection()

    def get_portfolio_symbols(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT symbol FROM my_portfolio")
            return [row[0] for row in cur.fetchall()]

    # 여기에 upsert_detected_signal, insert_history 등 
    # 우리가 기획한 메서드들을 추가해 나갈 것입니다.