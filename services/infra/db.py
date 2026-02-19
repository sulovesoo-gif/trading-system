import os
import psycopg2
from contextlib import contextmanager
# from dotenv import load_dotenv
# load_dotenv()

def get_db_params():
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "database": os.getenv("POSTGRES_DB", "trading_db"),
        "user": os.getenv("POSTGRES_USER", "admin"),
        "password": os.getenv("POSTGRES_PASSWORD", "stock123"),
        "port": int(os.getenv("DB_PORT", "5432")),
    }

@contextmanager
def db_conn():
    conn = psycopg2.connect(**get_db_params())
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()