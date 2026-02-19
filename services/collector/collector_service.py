from services.collector.candidates import collect_candidates_by_condition
from services.collector.prices import update_candidate_prices
from services.collector.daily_bars import collect_all_daily
from services.collector.daily_indicators import calculate_daily_indicators
from services.collector.minute_bars import collect_minute_bars_once

def run_once(auth, user_id: str, base_url: str, target_conditions: dict[str, str]):
    # 1) 후보 수집 (조건식 4/5/6)
    for name, seq in target_conditions.items():
        collect_candidates_by_condition(auth, user_id, base_url, seq, name)

    # 2) 후보 현재가 갱신
    update_candidate_prices(auth, base_url)

    # 3) 일봉 수집
    collect_all_daily(auth, base_url)

    # 4) 일봉 지표 계산/저장
    calculate_daily_indicators()

    # 5) 장중이면 1분봉 수집
    collect_minute_bars_once(auth, base_url)
