from services.infra.db import db_conn

AGG_5M_SQL = """
WITH src AS (
  SELECT
    code, ts, open_price, high_price, low_price, close_price, volume
  FROM stock_minute_bars
  WHERE code = %s
  ORDER BY ts DESC
  LIMIT %s
),
m AS (
  SELECT
    code,
    ts,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    date_trunc('minute', ts)
      - interval '1 minute' * (EXTRACT(minute FROM ts)::int %% 5) AS ts_5m
  FROM src
),
w AS (
  SELECT
    code,
    ts_5m,
    high_price,
    low_price,
    volume,
    FIRST_VALUE(open_price) OVER (
      PARTITION BY code, ts_5m
      ORDER BY ts ASC
    ) AS open_5m,
    LAST_VALUE(close_price) OVER (
      PARTITION BY code, ts_5m
      ORDER BY ts ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS close_5m
  FROM m
),
agg AS (
  SELECT
    code,
    ts_5m AS ts,
    MAX(open_5m)    AS open_price,
    MAX(close_5m)   AS close_price,
    MAX(high_price) AS high_price,
    MIN(low_price)  AS low_price,
    SUM(volume)     AS volume
  FROM w
  GROUP BY code, ts_5m
)
SELECT
  code, ts,
  open_price, high_price, low_price, close_price, volume
FROM agg
ORDER BY ts DESC
LIMIT %s;
"""

def fetch_5m_bars(code: str, bars_5m: int, raw_buffer: int = 50):
    if bars_5m <= 0:
        return []

    raw_limit = bars_5m * 5 + raw_buffer

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(AGG_5M_SQL, (code, raw_limit, bars_5m))
            rows = cur.fetchall()

    if not rows:
        return []

    rows.reverse()
    return rows
