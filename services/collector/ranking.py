from services.infra.db import db_conn

def get_trending_top_n(limit: int = 5, r2_th: float = 0.27):
    """
    detected_signals 기준 "현재 추세 유지" 종목 Top N
    - LRL > MA
    - LRS > 0
    - R² >= r2_th
    score 기준 정렬
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    code,
                    current_price,
                    current_score,
                    ma,
                    lrl_value,
                    lrl_slope,
                    r_square,
                    (lrl_value - ma) AS lrl_ma_diff
                FROM detected_signals
                WHERE lrl_value > ma
                  AND lrl_slope > 0
                  AND r_square >= %s
                ORDER BY current_score DESC NULLS LAST
                LIMIT %s
            """, (r2_th, limit))
            return cur.fetchall()


def get_top_by_score(limit: int = 5):
    """
    fallback: 조건 없이 score 상위.
    (추세 조건 충족 종목이 0개일 때라도 '뭐라도' 보여주기용)
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    code,
                    current_price,
                    current_score,
                    ma,
                    lrl_value,
                    lrl_slope,
                    r_square,
                    (lrl_value - ma) AS lrl_ma_diff
                FROM detected_signals
                ORDER BY current_score DESC NULLS LAST
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
