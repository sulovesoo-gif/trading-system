import os
import psycopg
from dotenv import load_dotenv

# .env 로드
load_dotenv(dotenv_path="../../.env")

def initialize_tables():
    try:
        # DB 연결
        conn = psycopg.connect(
            host="localhost",
            port=5432,
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        cur = conn.cursor()

        # 1. 일반 테이블 생성 (timestamp, 종목코드, 가격)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                price NUMERIC
            );
        """)

        # 2. TimescaleDB 하이퍼테이블로 변환 (이미 변환된 경우를 대비해 에러 무시)
        try:
            cur.execute("SELECT create_hypertable('stock_prices', 'timestamp', if_not_exists => TRUE);")
            print("✅ 하이퍼테이블 생성/확인 완료!")
        except Exception as e:
            # 이미 하이퍼테이블인 경우 에러가 날 수 있으므로 가볍게 넘깁니다.
            print(f"ℹ️ 하이퍼테이블 알림: {e}")

        conn.commit()
        cur.close()
        conn.close()
        print("🚀 DB 테이블 준비가 모두 끝났습니다!")

    except Exception as e:
        print(f"❌ DB 초기화 실패: {e}")

if __name__ == "__main__":
    initialize_tables()