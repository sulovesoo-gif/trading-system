import numpy as np
from scipy.stats import linregress


def compute_daily_indicators(
    close_prices: list[float],
    volumes: list[float],
    period: int,
    bb_period: int,
    bb_std: float,
):
    """
    순수 계산 함수.
    - DB/requests 모름
    - 입력: close_prices, volumes (시간 오름차순)
    - 출력: dict
    """
    if len(close_prices) < max(period, bb_period) or len(volumes) < period:
        return None

    closes = np.array(close_prices, dtype=float)
    vols = np.array(volumes, dtype=float)

    # MA(period)
    ma = float(np.mean(closes[-period:]))

    # Volume avg/spike (period)
    volume_avg = float(np.mean(vols[-period:]))
    volume_spike = float(vols[-1] / volume_avg) if volume_avg > 0 else 0.0

    # Bollinger (bb_period, bb_std)
    bb_mid = float(np.mean(closes[-bb_period:]))
    bb_sigma = float(np.std(closes[-bb_period:], ddof=0))
    bb_upper = bb_mid + bb_std * bb_sigma
    bb_lower = bb_mid - bb_std * bb_sigma

    # LRL/LRS/R² (period) - linear regression on last period closes
    y = closes[-period:]
    x = np.arange(period)
    slope, intercept, r_value, _, _ = linregress(x, y)
    lrl_value = float(intercept + slope * (period - 1))
    lrl_slope = float(slope)
    r_square = float(r_value ** 2)

    close_now = float(closes[-1])
    close_prev = float(closes[-2])

    # 이벤트 정의(일봉 close 기준)
    bb_upper_touch = close_now >= bb_upper
    bb_upper_break = (close_prev < bb_upper) and (close_now >= bb_upper)

    bb_lower_touch = close_now <= bb_lower
    bb_lower_break = (close_prev > bb_lower) and (close_now <= bb_lower)

    return {
        "ma": ma,
        "lrl_value": lrl_value,
        "lrl_slope": lrl_slope,
        "r_square": r_square,
        "bb_upper": float(bb_upper),
        "bb_lower": float(bb_lower),
        "volume_avg": volume_avg,
        "volume_spike": volume_spike,
        "bb_upper_touch": bool(bb_upper_touch),
        "bb_upper_break": bool(bb_upper_break),
        "bb_lower_touch": bool(bb_lower_touch),
        "bb_lower_break": bool(bb_lower_break),
    }
