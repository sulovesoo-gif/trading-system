from __future__ import annotations

import os

from services.collector.common import get_tracked_codes
from services.collector.minute_bars_agg import fetch_5m_bars
from services.collector.ranking import get_trending_top_n
from services.collector.signals import (
    get_prev_signal_state,
    upsert_detected_signal,
    upsert_signal_history_row,
    get_position_state_from_history,
    get_last_buy_entry,
    get_peak_price_since,
)
from services.collector import PERIOD, BB_PERIOD, BB_STD
from services.indicators.daily import compute_daily_indicators
from services.utils.signal_message import build_signal_message, build_trending_message
from services.utils.telegram import send_telegram, should_send
from services.infra.market_time import is_market_open

# --- BUY 필터 임계값 (V2에서 사용) ---
VOL_SPIKE_TH = 1.8
R2_TH = 0.27

# --- 리스크/익절 운영 규칙 ---
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.05"))      # 손절 -5% 무조건
PROFIT_ARM_PCT = float(os.getenv("PROFIT_ARM_PCT", "0.05"))    # +5% 이상 구간에서만 매도 허용
TRAIL_PCT = float(os.getenv("TRAIL_PCT", "0.05"))              # +5% 이상 구간에서만 트레일링(-5%) 활성


def _profit_armed(entry_price: int, peak_price: int | None) -> bool:
    """진입 후 최고가가 +5% 이상 찍혔는지(익절 운영 구간 진입 여부)."""
    if entry_price is None or entry_price <= 0:
        return False
    if peak_price is None:
        return False
    return peak_price >= entry_price * (1.0 + PROFIT_ARM_PCT)


def _calc_exit_flags(
    code: str,
    pos_state: str,
    last_price: int,
    cross_down: bool,
    slope_down: bool,
    table_name: str,
):
    """
    공통 청산 플래그 계산 (BASE/V2 동일 규칙)
    - 손절: -5% 무조건
    - 익절 운영(+5% 찍은 이후) 전에는 매도 금지
    - 익절 운영 구간에서만:
        - 기술매도: cross_down OR slope_down (기존 로직 유지)
        - 트레일링: 최고가 대비 -5%
    """
    if pos_state != "LONG":
        return {
            "sell": False,
            "stop_loss": False,
            "profit_armed": False,
            "technical": False,
            "trailing": False,
            "entry_ts": None,
            "entry_price": None,
            "peak_price": None,
        }

    entry = get_last_buy_entry(code, table_name=table_name)
    if not entry:
        # LONG인데 entry가 없으면 방어적으로 매도하지 않음(데이터 꼬임 방지)
        return {
            "sell": False,
            "stop_loss": False,
            "profit_armed": False,
            "technical": False,
            "trailing": False,
            "entry_ts": None,
            "entry_price": None,
            "peak_price": None,
        }

    entry_ts, entry_price = entry
    peak_price = get_peak_price_since(code, entry_ts, table_name=table_name)

    # 1) 손절은 무조건
    stop_loss_hit = (last_price <= entry_price * (1.0 - STOP_LOSS_PCT))

    # 2) 익절 운영 구간(+5% 찍은 이후) 여부
    armed = _profit_armed(entry_price, peak_price)

    # 3) 익절 운영 구간에서만 기술매도/트레일링
    technical_sell = bool(armed and (cross_down or slope_down))

    trailing_hit = False
    if armed and peak_price and peak_price > 0:
        trailing_hit = (last_price <= peak_price * (1.0 - TRAIL_PCT))

    sell = bool(stop_loss_hit or technical_sell or trailing_hit)

    return {
        "sell": sell,
        "stop_loss": stop_loss_hit,
        "profit_armed": armed,
        "technical": technical_sell,
        "trailing": trailing_hit,
        "entry_ts": entry_ts,
        "entry_price": entry_price,
        "peak_price": peak_price,
    }


def calc_minute_signals(limit: int = 300) -> int:
    """
    [BASE]
      - BUY: (FLAT) prev_lrl<=prev_ma AND curr_lrl>curr_ma AND curr_slope>0
      - SELL:
          * -5% 손절 무조건
          * +5% 구간 진입(최고가 기준) 전에는 매도 금지
          * +5% 구간에서만 (cross_down OR slope_down) 매도 허용
          * +5% 구간에서만 트레일링(-5%) 활성
      - 상태: detected_signals.position_state (FLAT/LONG)
      - 기록: signal_history

    [V2 비교용]
      - BUY: BASE_BUY AND (r_square>=0.27 OR volume_spike>=1.8)
      - SELL: BASE와 동일 규칙(위 SELL)
      - 상태: signal_history_v2에서 마지막 BUY/SELL로 판단
      - 기록: signal_history_v2
    """
    print("▶ calc_minute_signals 진입")

    codes = get_tracked_codes(limit=limit)
    if not codes:
        print("?? 추적 종목 없음")
        return 0

    lookback = max(PERIOD, BB_PERIOD)
    saved = 0

    for code in codes:
        bars = fetch_5m_bars(code, bars_5m=lookback)
        if not bars or len(bars) < lookback:
            continue

        ts = bars[-1][1]
        closes = [float(r[5]) for r in bars]
        vols = [float(r[6]) for r in bars]
        last_price = int(bars[-1][5])
        last_volume = int(bars[-1][6])

        out = compute_daily_indicators(
            close_prices=closes,
            volumes=vols,
            period=PERIOD,
            bb_period=BB_PERIOD,
            bb_std=BB_STD,
        )
        if out is None:
            continue

        # ======================================================
        # 공통: 직전 지표(기존 BASE는 detected_signals에 저장된 직전값 사용)
        # ======================================================
        prev = get_prev_signal_state(code)
        pos_base = prev.get("position_state", "FLAT")

        # 기존 교차 조건(너가 쓰던 그 조건)
        cross_up = (prev.get("lrl_value", 0) <= prev.get("ma", 0)) and (out["lrl_value"] > out["ma"])
        cross_down = (prev.get("lrl_value", 0) >= prev.get("ma", 0)) and (out["lrl_value"] < out["ma"])
        slope_down = (prev.get("lrl_slope", 0) >= 0) and (out["lrl_slope"] < 0)

        # ======================================================
        # BASE: BUY/SELL
        # ======================================================
        base_buy_event = True if (pos_base == "FLAT" and cross_up and out["lrl_slope"] > 0) else None

        base_exit = _calc_exit_flags(
            code=code,
            pos_state=pos_base,
            last_price=last_price,
            cross_down=bool(cross_down),
            slope_down=bool(slope_down),
            table_name="signal_history",
        )
        base_sell_event = True if base_exit["sell"] else None

        # BASE 상태 전이
        next_pos = pos_base
        if base_buy_event:
            next_pos = "LONG"
        elif base_sell_event:
            next_pos = "FLAT"

        # ======================================================
        # 이벤트/점수(대시보드용)
        # ======================================================
        # (이전 이벤트 상태는 detected_signals의 boolean을 사용)
        bb_upper_break_event = True if (not prev["bb_upper_break"] and out["bb_upper_break"]) else None
        bb_lower_break_event = True if (not prev["bb_lower_break"] and out["bb_lower_break"]) else None
        bb_upper_touch_event = True if (not prev["bb_upper_touch"] and out["bb_upper_touch"]) else None
        bb_lower_touch_event = True if (not prev["bb_lower_touch"] and out["bb_lower_touch"]) else None

        # “현재값” 기준으로도 표시하고 싶으면 out값 그대로 저장(아래 upsert에서 저장됨)
        vol_spike_now = float(out["volume_spike"])
        r2_now = float(out["r_square"])

        score = float(
            (3.0 if bb_upper_break_event else 0.0)
            + (1.5 if bb_upper_touch_event else 0.0)
            + (1.0 if vol_spike_now >= VOL_SPIKE_TH else 0.0)
            + (1.0 if r2_now >= R2_TH else 0.0)
            + (2.0 if base_buy_event else 0.0)
            - (2.0 if base_sell_event else 0.0)
        )

        # ======================================================
        # 1) BASE 기록: signal_history
        #    - 이번엔 지표를 "항상" 저장(희소 데이터 때문에 백필/시뮬이 꼬이는 거 방지)
        # ======================================================
        upsert_signal_history_row(
            code=code,
            ts=ts,
            price=last_price,
            volume=last_volume,
            score=score,
            bb_upper_break=bb_upper_break_event,
            bb_lower_break=bb_lower_break_event,
            bb_upper_touch=bb_upper_touch_event,
            bb_lower_touch=bb_lower_touch_event,
            volume_spike=vol_spike_now,
            r_square=r2_now,
            lrl_slope=float(out["lrl_slope"]),
            ma=float(out["ma"]),
            lrl_value=float(out["lrl_value"]),
            buy_signal=(True if base_buy_event else None),
            sell_signal=(True if base_sell_event else None),
            table_name="signal_history",
        )

        # BASE 스냅샷 업데이트 + position_state 저장
        upsert_detected_signal(
            code=code,
            ts=ts,
            price=last_price,
            volume=last_volume,
            score=score,
            bb_upper_break=bool(out["bb_upper_break"]),
            bb_lower_break=bool(out["bb_lower_break"]),
            bb_upper_touch=bool(out["bb_upper_touch"]),
            bb_lower_touch=bool(out["bb_lower_touch"]),
            volume_spike=vol_spike_now,
            r_square=r2_now,
            lrl_slope=float(out["lrl_slope"]),
            ma=float(out["ma"]),
            lrl_value=float(out["lrl_value"]),
            position_state=next_pos,
            signal_ts=(ts if (base_buy_event or base_sell_event) else None),
        )

        # 텔레그램 (BASE만)
        if base_buy_event and should_send(code, "BUY", cooldown_sec=600):
            msg = build_signal_message(
                signal_type="BUY",
                code=code,
                ts=ts,
                last_price=last_price,
                last_volume=last_volume,
                score=score,
                out=out,
                reasons=[
                    "LRL(14) ↑ MA(14) 상향 돌파",
                    "LRS(slope) > 0",
                ],
            )
            send_telegram(msg)

        if base_sell_event and should_send(code, "SELL", cooldown_sec=600):
            reasons = []
            if base_exit["stop_loss"]:
                reasons.append(f"손절(-{STOP_LOSS_PCT*100:.0f}%) 무조건")
            else:
                # 손절이 아닌 경우는 무조건 +5% 구간에서만 가능
                if base_exit["profit_armed"]:
                    if base_exit["technical"]:
                        # 기존 매도조건 그대로(합치지 않음)
                        if cross_down:
                            reasons.append("LRL(14) ↓ MA(14) 하향 돌파")
                        if slope_down:
                            reasons.append("LRS 0선 하향 돌파(기울기 음수 전환)")
                    if base_exit["trailing"]:
                        reasons.append(f"트레일링(-{TRAIL_PCT*100:.0f}%) (익절 구간에서만)")
                else:
                    reasons.append("익절(+5%) 구간 전에는 매도 금지(규칙 위반 방지용)")

            msg = build_signal_message(
                signal_type="SELL",
                code=code,
                ts=ts,
                last_price=last_price,
                last_volume=last_volume,
                score=score,
                out=out,
                reasons=reasons or ["매도 조건 충족"],
            )
            send_telegram(msg)

        # ======================================================
        # 2) V2 비교용 기록: signal_history_v2
        #    - BUY만 보수적으로: BASE_BUY AND (R² OR VolumeSpike)
        #    - SELL은 BASE와 동일 규칙(손절/익절+기술/트레일링)
        # ======================================================
        pos_v2 = get_position_state_from_history(code, table_name="signal_history_v2")
        v2_buy_filter = (r2_now >= R2_TH) or (vol_spike_now >= VOL_SPIKE_TH)
        v2_buy_event = True if (pos_v2 == "FLAT" and cross_up and out["lrl_slope"] > 0 and v2_buy_filter) else None

        v2_exit = _calc_exit_flags(
            code=code,
            pos_state=pos_v2,
            last_price=last_price,
            cross_down=bool(cross_down),
            slope_down=bool(slope_down),
            table_name="signal_history_v2",
        )
        v2_sell_event = True if v2_exit["sell"] else None

        upsert_signal_history_row(
            code=code,
            ts=ts,
            price=last_price,
            volume=last_volume,
            score=score,
            bb_upper_break=bb_upper_break_event,
            bb_lower_break=bb_lower_break_event,
            bb_upper_touch=bb_upper_touch_event,
            bb_lower_touch=bb_lower_touch_event,
            volume_spike=vol_spike_now,
            r_square=r2_now,
            lrl_slope=float(out["lrl_slope"]),
            ma=float(out["ma"]),
            lrl_value=float(out["lrl_value"]),
            buy_signal=(True if v2_buy_event else None),
            sell_signal=(True if v2_sell_event else None),
            table_name="signal_history_v2",
        )

        saved += 1

    # ======================================================
    # TOP5 (장중만 / BASE 기준으로만 전송)
    # ======================================================
    if is_market_open() and should_send("TRENDING", "TOP5", cooldown_sec=600):
        rows = get_trending_top_n(limit=5, r2_th=R2_TH)
        if rows:
            msg = build_trending_message(rows, title="📊 현재 추세 상위 TOP5 (BASE)")
            send_telegram(msg)

    print(f"✅ minute signals saved: {saved}")
    return saved
