from __future__ import annotations

from services.infra.db import db_conn


# ============================================================
# detected_signals: BASE 전략 상태/스냅샷
# ============================================================

def get_prev_signal_state(code: str) -> dict:
    """
    detected_signals의 직전 상태(스냅샷)를 읽음.
    - BASE 전략에서 FLAT/LONG 상태 및 직전 지표값을 유지하는 용도
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(bb_upper_break, FALSE),
                    COALESCE(bb_lower_break, FALSE),
                    COALESCE(bb_upper_touch, FALSE),
                    COALESCE(bb_lower_touch, FALSE),
                    COALESCE(volume_spike, 0),
                    COALESCE(r_square, 0),
                    COALESCE(ma, 0),
                    COALESCE(lrl_value, 0),
                    COALESCE(lrl_slope, 0),
                    COALESCE(position_state, 'FLAT')
                FROM detected_signals
                WHERE code = %s
                """,
                (code,),
            )
            row = cur.fetchone()

    if not row:
        return {
            "bb_upper_break": False,
            "bb_lower_break": False,
            "bb_upper_touch": False,
            "bb_lower_touch": False,
            "volume_spike": 0.0,
            "r_square": 0.0,
            "ma": 0.0,
            "lrl_value": 0.0,
            "lrl_slope": 0.0,
            "position_state": "FLAT",
        }

    return {
        "bb_upper_break": bool(row[0]),
        "bb_lower_break": bool(row[1]),
        "bb_upper_touch": bool(row[2]),
        "bb_lower_touch": bool(row[3]),
        "volume_spike": float(row[4]),
        "r_square": float(row[5]),
        "ma": float(row[6]),
        "lrl_value": float(row[7]),
        "lrl_slope": float(row[8]),
        "position_state": str(row[9]),
    }


def upsert_detected_signal(
    code: str,
    ts,
    price: int,
    volume: int,
    score: float,
    bb_upper_break: bool | None,
    bb_lower_break: bool | None,
    bb_upper_touch: bool | None,
    bb_lower_touch: bool | None,
    volume_spike: float | None,
    r_square: float | None,
    lrl_slope: float | None,
    ma: float | None,
    lrl_value: float | None,
    position_state: str | None = None,  # FLAT/LONG
    signal_ts=None,
) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO detected_signals (
                    code, last_checked_ts,
                    current_price, current_volume, current_score,
                    bb_upper_break, bb_lower_break,
                    bb_upper_touch, bb_lower_touch,
                    volume_spike, r_square, lrl_slope,
                    ma, lrl_value,
                    position_state,
                    signal_ts, updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (code) DO UPDATE SET
                    last_checked_ts = EXCLUDED.last_checked_ts,
                    current_price   = EXCLUDED.current_price,
                    current_volume  = EXCLUDED.current_volume,
                    current_score   = EXCLUDED.current_score,
                    bb_upper_break  = EXCLUDED.bb_upper_break,
                    bb_lower_break  = EXCLUDED.bb_lower_break,
                    bb_upper_touch  = EXCLUDED.bb_upper_touch,
                    bb_lower_touch  = EXCLUDED.bb_lower_touch,
                    volume_spike    = EXCLUDED.volume_spike,
                    r_square        = EXCLUDED.r_square,
                    lrl_slope       = EXCLUDED.lrl_slope,
                    ma              = EXCLUDED.ma,
                    lrl_value       = EXCLUDED.lrl_value,
                    position_state  = COALESCE(EXCLUDED.position_state, detected_signals.position_state),
                    signal_ts       = COALESCE(EXCLUDED.signal_ts, detected_signals.signal_ts),
                    updated_at      = NOW()
                """,
                (
                    code,
                    ts,
                    price,
                    volume,
                    score,
                    bb_upper_break,
                    bb_lower_break,
                    bb_upper_touch,
                    bb_lower_touch,
                    volume_spike,
                    r_square,
                    lrl_slope,
                    ma,
                    lrl_value,
                    position_state,
                    signal_ts,
                ),
            )
        conn.commit()


# ============================================================
# signal_history / signal_history_v2: 공용 upsert
# ============================================================

def upsert_signal_history_row(
    code: str,
    ts,
    price: int,
    volume: int,
    score: float,
    bb_upper_break: bool | None = None,
    bb_lower_break: bool | None = None,
    bb_upper_touch: bool | None = None,
    bb_lower_touch: bool | None = None,
    volume_spike: float | None = None,
    r_square: float | None = None,
    lrl_slope: float | None = None,
    ma: float | None = None,
    lrl_value: float | None = None,
    buy_signal: bool | None = None,
    sell_signal: bool | None = None,
    table_name: str = "signal_history",
) -> None:
    """
    signal_history / signal_history_v2 둘 다 지원.
    table_name은 허용값만 통과.
    """
    if table_name not in ("signal_history", "signal_history_v2"):
        raise ValueError(f"Invalid table_name: {table_name}")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table_name} (
                    code, ts,
                    price, volume, score,
                    bb_upper_break, bb_lower_break,
                    bb_upper_touch, bb_lower_touch,
                    volume_spike, r_square, lrl_slope,
                    ma, lrl_value,
                    buy_signal, sell_signal
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (code, ts) DO UPDATE SET
                    price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    score = EXCLUDED.score,
                    bb_upper_break = COALESCE(EXCLUDED.bb_upper_break, {table_name}.bb_upper_break),
                    bb_lower_break = COALESCE(EXCLUDED.bb_lower_break, {table_name}.bb_lower_break),
                    bb_upper_touch = COALESCE(EXCLUDED.bb_upper_touch, {table_name}.bb_upper_touch),
                    bb_lower_touch = COALESCE(EXCLUDED.bb_lower_touch, {table_name}.bb_lower_touch),
                    volume_spike   = COALESCE(EXCLUDED.volume_spike  , {table_name}.volume_spike),
                    r_square       = COALESCE(EXCLUDED.r_square      , {table_name}.r_square),
                    lrl_slope      = COALESCE(EXCLUDED.lrl_slope     , {table_name}.lrl_slope),
                    ma             = COALESCE(EXCLUDED.ma            , {table_name}.ma),
                    lrl_value      = COALESCE(EXCLUDED.lrl_value     , {table_name}.lrl_value),
                    buy_signal     = COALESCE(EXCLUDED.buy_signal    , {table_name}.buy_signal),
                    sell_signal    = COALESCE(EXCLUDED.sell_signal   , {table_name}.sell_signal)
                """,
                (
                    code,
                    ts,
                    price,
                    volume,
                    score,
                    bb_upper_break,
                    bb_lower_break,
                    bb_upper_touch,
                    bb_lower_touch,
                    volume_spike,
                    r_square,
                    lrl_slope,
                    ma,
                    lrl_value,
                    buy_signal,
                    sell_signal,
                ),
            )
        conn.commit()


def get_position_state_from_history(code: str, table_name: str = "signal_history_v2") -> str:
    """
    V2는 detected_signals를 따로 두지 않으므로 history에서 마지막 매수/매도를 보고 상태를 판단.
    - 마지막 신호가 BUY면 LONG
    - 마지막 신호가 SELL이면 FLAT
    - 신호 없으면 FLAT
    """
    if table_name not in ("signal_history", "signal_history_v2"):
        raise ValueError(f"Invalid table_name: {table_name}")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  CASE
                    WHEN buy_signal IS TRUE THEN 'LONG'
                    WHEN sell_signal IS TRUE THEN 'FLAT'
                    ELSE NULL
                  END AS state
                FROM {table_name}
                WHERE code = %s
                  AND (buy_signal IS TRUE OR sell_signal IS TRUE)
                ORDER BY ts DESC
                LIMIT 1
                """,
                (code,),
            )
            row = cur.fetchone()

    if not row or not row[0]:
        return "FLAT"
    return str(row[0])


# ============================================================
# 손절/트레일링 계산용 DB 헬퍼 (BASE 전략에서 사용)
# - 스키마 추가 없이 signal_history 기반으로 계산
# ============================================================

def get_last_buy_entry(code: str, table_name: str = "signal_history"):
    """
    마지막 BUY 신호(진입) 1건
    return: (entry_ts, entry_price) or None
    """
    if table_name not in ("signal_history", "signal_history_v2"):
        raise ValueError(f"Invalid table_name: {table_name}")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ts, price
                FROM {table_name}
                WHERE code = %s
                  AND buy_signal IS TRUE
                ORDER BY ts DESC
                LIMIT 1
                """,
                (code,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return row[0], int(row[1])


def get_peak_price_since(code: str, entry_ts, table_name: str = "signal_history"):
    """
    진입 이후 구간에서의 최고가 (price 기준)
    return: int or None
    """
    if table_name not in ("signal_history", "signal_history_v2"):
        raise ValueError(f"Invalid table_name: {table_name}")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT MAX(price)
                FROM {table_name}
                WHERE code = %s
                  AND ts >= %s
                """,
                (code, entry_ts),
            )
            row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])