import os

from services.collector.signals import (
    get_prev_signal_state,
    upsert_detected_signal,
    upsert_signal_history_row,
    get_last_buy_entry,
    get_peak_price_since,
)
from services.collector import PERIOD, BB_PERIOD, BB_STD
from services.indicators.daily import compute_daily_indicators
from services.collector.minute_bars_agg import fetch_5m_bars
from services.collector.ranking import get_trending_top_n, get_top_by_score
from services.utils.telegram import send_telegram, should_send
from services.utils.signal_message import build_signal_message, build_trending_message
from services.infra.market_time import is_market_open

# 튜닝 파라미터
VOL_SPIKE_TH = 1.8
R2_TH = 0.27
SLOPE_TH = 0.0

# 리스크 파라미터
TRAIL_PCT = 0.05  # ✅ 최고가 대비 -5% (확정)
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.05"))  # 손절 기본 5% (필요시 env로 조절)


def calc_minute_signals(limit: int = 300) -> int:
    """
    1) 추적 종목(top N) 가져오기
    2) 종목별 최근 5분봉(집계)로 지표 계산
    3) detected_signals 업데이트 + signal_history 기록
    4) 텔레그램: BUY/SELL은 position_state 기준 1회, TOP5는 10분마다(장중만)
    """
    print("▶ calc_minute_signals 진입")

    # get_tracked_codes가 어디서 오든, 기존 코드에 맞춰 유지
    from services.collector.common import get_tracked_codes
    codes = get_tracked_codes(limit=limit)
    if not codes:
        print("?? 추적 종목 없음")
        return 0

    lookback = max(PERIOD, BB_PERIOD)
    saved = 0

    for code in codes:
        # 5분봉 lookback개
        bars = fetch_5m_bars(code, bars_5m=lookback)
        if not bars or len(bars) < lookback:
            continue

        # bars: (code, ts, open, high, low, close, volume)
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

        prev = get_prev_signal_state(code)
        pos = prev.get("position_state", "FLAT")  # FLAT / LONG

        # ======================================================
        # 1) BUY / 기본 SELL (LRL/MA + slope)
        # ======================================================
        cross_up = (prev.get("lrl_value", 0) <= prev.get("ma", 0)) and (out["lrl_value"] > out["ma"])
        buy_event = True if (pos == "FLAT" and cross_up and out["lrl_slope"] > 0) else None

        cross_down = (prev.get("lrl_value", 0) >= prev.get("ma", 0)) and (out["lrl_value"] < out["ma"])
        slope_down = (prev.get("lrl_slope", 0) >= 0) and (out["lrl_slope"] < 0)

        # ======================================================
        # 2) 하락 패턴(매도조건) 묶음: 손절/트레일링(-5%)/BB하단Break
        #    - LONG일 때만 의미 있음
        # ======================================================
        entry_ts = None
        entry_price = None
        peak_price = None

        stop_loss_hit = False
        trailing_hit = False

        if pos == "LONG":
            entry = get_last_buy_entry(code)
            if entry:
                entry_ts, entry_price = entry
                peak_price = get_peak_price_since(code, entry_ts)

                # 손절: 진입가 대비 -STOP_LOSS_PCT
                if entry_price is not None and entry_price > 0:
                    stop_price = entry_price * (1.0 - STOP_LOSS_PCT)
                    stop_loss_hit = (last_price <= stop_price)

                # 트레일링: 최고가 대비 -5%
                if peak_price is not None and peak_price > 0:
                    trail_price = peak_price * (1.0 - TRAIL_PCT)
                    trailing_hit = (last_price <= trail_price)

        # BB 하단 break 이벤트(강한 하락 시그널로 사용)
        bb_lower_break_event = True if (not prev["bb_lower_break"] and out["bb_lower_break"]) else None

        # 최종 SELL 이벤트: 기존 (cross_down or slope_down) + 손절 + 트레일링 + bb_lower_break
        sell_reasons_flags = {
            "cross_down": bool(cross_down),
            "slope_down": bool(slope_down),
            "stop_loss": bool(stop_loss_hit),
            "trailing": bool(trailing_hit),
            "bb_lower_break": bool(bb_lower_break_event),
        }
        sell_event = True if (pos == "LONG" and (
            cross_down
            or slope_down
            or stop_loss_hit
            or trailing_hit
            or bb_lower_break_event
        )) else None

        # 포지션 상태 전이
        next_pos = pos
        if buy_event:
            next_pos = "LONG"
        elif sell_event:
            next_pos = "FLAT"

        # ======================================================
        # 3) 참고 이벤트들
        # ======================================================
        bb_upper_break_event = True if (not prev["bb_upper_break"] and out["bb_upper_break"]) else None
        bb_upper_touch_event = True if (not prev["bb_upper_touch"] and out["bb_upper_touch"]) else None
        bb_lower_touch_event = True if (not prev["bb_lower_touch"] and out["bb_lower_touch"]) else None

        vol_spike_event = out["volume_spike"] if (prev["volume_spike"] < VOL_SPIKE_TH and out["volume_spike"] >= VOL_SPIKE_TH) else None
        r2_event = (
            out["r_square"]
            if (prev["r_square"] < R2_TH and out["r_square"] >= R2_TH and out["lrl_slope"] > SLOPE_TH)
            else None
        )

        # 점수(임시)
        score = float(
            (3.0 if bb_upper_break_event else 0.0)
            + (1.5 if bb_upper_touch_event else 0.0)
            + (1.5 if r2_event is not None else 0.0)
            + (1.0 if vol_spike_event is not None else 0.0)
            + (2.0 if buy_event else 0.0)
            - (2.0 if sell_event else 0.0)
            - (1.0 if bb_lower_break_event else 0.0)
        )

        any_event = any([
            buy_event, sell_event,
            bb_upper_break_event, bb_lower_break_event,
            bb_upper_touch_event, bb_lower_touch_event,
            (vol_spike_event is not None),
            (r2_event is not None),
        ])

        # ======================================================
        # 4) signal_history 기록 (PK: code, ts)
        # ======================================================
        upsert_signal_history_row(
            code=code, ts=ts,
            price=last_price, volume=last_volume, score=score,

            # 이벤트 순간만 남기기
            bb_upper_break=bb_upper_break_event,
            bb_lower_break=bb_lower_break_event,
            bb_upper_touch=bb_upper_touch_event,
            bb_lower_touch=bb_lower_touch_event,
            volume_spike=vol_spike_event,
            r_square=r2_event,

            lrl_slope=(out["lrl_slope"] if any_event else None),
            ma=(out["ma"] if any_event else None),
            lrl_value=(out["lrl_value"] if any_event else None),

            buy_signal=(True if buy_event else None),
            sell_signal=(True if sell_event else None),
        )

        # ======================================================
        # 5) 텔레그램: BUY/SELL (쿨다운 600s)
        # ======================================================
        if buy_event and should_send(code, "BUY", cooldown_sec=600):
            reasons = [
                "LRL(14) ↑ MA(14) 상향 돌파",
                "LRS(slope) > 0 (상승 기울기)",
            ]
            msg = build_signal_message(
                signal_type="BUY",
                code=code,
                ts=ts,
                last_price=last_price,
                last_volume=last_volume,
                score=score,
                out=out,
                reasons=reasons,
            )
            send_telegram(msg)

        if sell_event and should_send(code, "SELL", cooldown_sec=600):
            reasons = []
            if sell_reasons_flags["cross_down"]:
                reasons.append("LRL(14) ↓ MA(14) 하향 돌파")
            if sell_reasons_flags["slope_down"]:
                reasons.append("LRS 0선 하향 돌파(기울기 음수 전환)")
            if sell_reasons_flags["stop_loss"]:
                reasons.append(f"손절({STOP_LOSS_PCT*100:.1f}%) 충족")
            if sell_reasons_flags["trailing"]:
                reasons.append("트레일링(-5%) 충족")
            if sell_reasons_flags["bb_lower_break"]:
                reasons.append("BB 하단 Break(하락 가속)")

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
        # 6) detected_signals 스냅샷 + position_state 저장
        # ======================================================
        upsert_detected_signal(
            code=code, ts=ts,
            price=last_price, volume=last_volume, score=score,

            bb_upper_break=out["bb_upper_break"],
            bb_lower_break=out["bb_lower_break"],
            bb_upper_touch=out["bb_upper_touch"],
            bb_lower_touch=out["bb_lower_touch"],
            volume_spike=out["volume_spike"],
            r_square=out["r_square"],
            lrl_slope=out["lrl_slope"],
            ma=out["ma"],
            lrl_value=out["lrl_value"],

            position_state=next_pos,
            signal_ts=(ts if any_event else None),
        )

        saved += 1

    # ======================================================
    # 7) 10분마다 TOP5 (장중만)
    # ======================================================
    if is_market_open() and should_send("TRENDING", "TOP5", cooldown_sec=600):
        rows = get_trending_top_n(limit=5, r2_th=R2_TH)

        if not rows:
            rows = get_top_by_score(limit=5)
            msg = build_trending_message(rows, title="📊 (Fallback) 점수 상위 TOP5")
        else:
            msg = build_trending_message(rows, title="📊 현재 추세 상위 TOP5 (LRL>MA & slope>0 & R² 기준)")

        send_telegram(msg)

    print(f"✅ minute signals saved: {saved}")
    return saved
