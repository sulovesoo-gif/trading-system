from datetime import datetime
from typing import Dict, Any, Optional

from services.utils.telegram import fmt_num, fmt_int

def build_signal_message(
    *,
    signal_type: str,              # "BUY" | "SELL" | "STOP" | "TRAIL"
    code: str,
    ts: datetime,
    last_price: int,
    last_volume: int,
    score: float,
    out: Dict[str, Any],           # indicators output (lrl/ma/r2/volume_spike/bb...)
    reasons: list[str],            # 충족 조건 설명 리스트
    extras: Optional[Dict[str, Any]] = None,  # entry/peak/pnl 등 추가 정보
) -> str:
    extras = extras or {}

    header = f"{'🟢' if signal_type=='BUY' else '🔴' if signal_type in ('SELL','STOP') else '🟠'} {signal_type} SIGNAL"
    lines = [
        header,
        f"종목: {code}",
        f"시각: {ts.strftime('%Y-%m-%d %H:%M:%S')}",
        f"가격: {fmt_int(last_price)} / 거래량(5m): {fmt_int(last_volume)}",
        f"점수: {fmt_num(score, 2)}",
        "",
        "✅ 이유",
    ]
    for r in reasons:
        lines.append(f"- {r}")

    # 핵심 수치(네가 보는 지표)
    lines += [
        "",
        "📌 지표(현재)",
        f"- MA(14): {fmt_num(out.get('ma'), 2)}",
        f"- LRL(14): {fmt_num(out.get('lrl_value'), 2)}",
        f"- LRS(slope): {fmt_num(out.get('lrl_slope'), 4)}",
        f"- R²: {fmt_num(out.get('r_square'), 3)}",
        f"- VolSpike: {fmt_num(out.get('volume_spike'), 2)}",
        f"- BB upper/lower: {fmt_num(out.get('bb_upper'), 2)} / {fmt_num(out.get('bb_lower'), 2)}",
    ]

    # 추가 상태(진입가/손익/최고가 등)
    if extras:
        lines += ["", "📎 상태"]
        for k, v in extras.items():
            lines.append(f"- {k}: {v}")

    return "\n".join(lines)

def build_trending_message(rows, title: str = "📊 현재 추세 상위 종목 TOP5"):
    if not rows:
        return f"{title}\n\n(조건 충족 종목 없음)"

    lines = [title, ""]

    for idx, r in enumerate(rows, start=1):
        # r = (code, price, score, ma, lrl, slope, r2, diff)
        code, price, score, ma, lrl, slope, r2, diff = r

        try:
            price_s = f"₩{int(price):,}" if price is not None else "-"
        except Exception:
            price_s = str(price)

        try:
            score_s = f"{float(score):.2f}" if score is not None else "-"
        except Exception:
            score_s = str(score)

        try:
            r2_s = f"{float(r2):.2f}" if r2 is not None else "-"
        except Exception:
            r2_s = str(r2)

        try:
            slope_s = f"{float(slope):.4f}" if slope is not None else "-"
        except Exception:
            slope_s = str(slope)

        try:
            diff_s = f"{float(diff):.2f}" if diff is not None else "-"
        except Exception:
            diff_s = str(diff)

        lines.append(
            f"{idx}. {code}  {price_s}\n"
            f"   score:{score_s}  R²:{r2_s}  slope:{slope_s}  (LRL-MA):{diff_s}"
        )

    return "\n".join(lines)