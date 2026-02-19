from services.infra.db import db_conn

def get_tracked_codes(limit: int = 300):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT code
                FROM candidate_stocks
                ORDER BY COALESCE(collected_at, trade_date::timestamptz) DESC
                LIMIT %s
            """, (limit,))
            return [r[0] for r in cur.fetchall()]
