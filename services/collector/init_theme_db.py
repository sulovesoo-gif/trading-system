from services.common.db_manager import get_db_connection

def init_theme_tables():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS themes (
                        theme_id SERIAL PRIMARY KEY,
                        theme_name TEXT UNIQUE NOT NULL,
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS theme_stocks (
                        theme_id INTEGER REFERENCES themes(theme_id) ON DELETE CASCADE,
                        symbol TEXT NOT NULL,
                        stock_name TEXT,
                        PRIMARY KEY (theme_id, symbol)
                    );
                """)
                print("✅ 테마 관련 테이블 생성 완료!")
    except Exception as e:
        print(f"❌ 테이블 생성 실패: {e}")

if __name__ == "__main__":
    init_theme_tables()