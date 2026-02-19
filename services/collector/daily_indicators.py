from datetime import datetime
from services.infra.db import db_conn
from services.indicators.daily import compute_daily_indicators
from services.collector import PERIOD, BB_PERIOD, BB_STD

def calculate_daily_indicators(period: int = PERIOD, bb_period: int = BB_PERIOD, bb_std: float = BB_STD):
    # DB에서 종목별 최근 max(period, bb_period)일만 읽어서 계산 (전체 read_sql보다 가볍고 안정적)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT code FROM stock_daily_bars")
            codes = [r[0] for r in cur.fetchall()]

    if not codes:
        print("⚠️ stock_daily_bars 비어 있음")
        return 0

    saved = 0

    with db_conn() as conn:
        with conn.cursor() as cur:
            for code in codes:
                cur.execute("""
                    SELECT trade_date, close_price, volume
                    FROM stock_daily_bars
                    WHERE code = %s
                    ORDER BY trade_date ASC
                """, (code,))
                rows = cur.fetchall()

                if len(rows) < max(period, bb_period):
                    continue

                # 마지막 max(period, bb_period)개만 사용
                rows = rows[-max(period, bb_period):]
                last_trade_date = rows[-1][0]
                closes = [float(r[1]) for r in rows]
                vols = [float(r[2]) for r in rows]

                out = compute_daily_indicators(
                    close_prices=closes,
                    volumes=vols,
                    period=period,
                    bb_period=bb_period,
                    bb_std=bb_std,
                )
                if out is None:
                    continue

                ts = datetime.combine(last_trade_date, datetime.min.time())

                cur.execute("""
                    INSERT INTO stock_indicators (
                        code, ts, period,
                        ma, lrl_value, lrl_slope, r_square,
                        bb_upper, bb_lower,
                        volume_avg, volume_spike,
                        bb_upper_touch, bb_upper_break,
                        bb_lower_touch, bb_lower_break
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (code, ts, period) DO UPDATE SET
                        ma = EXCLUDED.ma,
                        lrl_value = EXCLUDED.lrl_value,
                        lrl_slope = EXCLUDED.lrl_slope,
                        r_square = EXCLUDED.r_square,
                                           bb_upper = EXCLUDED.bb_upper,
                        bb_lower = EXCLUDED.bb_lower,
                        volume_avg = EXCLUDED.volume_avg,
                        volume_spike = EXCLUDED.volume_spike,
                        bb_upper_touch = EXCLUDED.bb_upper_touch,
                        bb_upper_break = EXCLUDED.bb_upper_break,
                        bb_lower_touch = EXCLUDED.bb_lower_touch,
                        bb_lower_break = EXCLUDED.bb_lower_break,
                        created_at = NOW()
                """, (
                    code, ts, period,
                    out["ma"], out["lrl_value"], out["lrl_slope"], out["r_square"],
                    out["bb_upper"], out["bb_lower"],
                    out["volume_avg"], out["volume_spike"],
                    out["bb_upper_touch"], out["bb_upper_break"],
                    out["bb_lower_touch"], out["bb_lower_break"],
                ))
                saved += 1

    print(f"✅ stock_indicators {saved}건 계산/저장 완료")
    return saved
