from dotenv import load_dotenv
load_dotenv()

import os
import time
import traceback
from datetime import datetime

from services.kis_auth import KISAuth
from services.collector.collector_service import run_once
from services.collector.minute_signals import calc_minute_signals
from services.infra.market_time import is_market_open

def main():
    auth = KISAuth()

    target_conditions = {
        "거래대금 상위 + 최소 시총": "4",
        "등락률 상위 + 거래대금": "5",
        "최근 5일 신고가 갱신": "6",
    }

    # ✅ 장외에도 강제로 실행할지 (테스트용)
    FORCE_RUN = os.getenv("FORCE_RUN", "0") == "1"

    fail_count = 0

    while True:
        try:
            now = datetime.now().strftime("%H:%M:%S")

            market_open = is_market_open()
            if market_open or FORCE_RUN:
                tag = "장중" if market_open else "장외(강제실행)"
                print(f"🕒 [{now}] {tag} → 수집/계산 실행")
                
                if FORCE_RUN:
                    calc_minute_signals(limit=300)
                else:
                    # 장외 강제 실행일 땐 조건식(장중 전용)에서 스킵될 수 있음(정상)
                    run_once(auth, auth.user_id, auth.base_url, target_conditions)
                    print("✅ run_once 끝, 이제 calc_minute_signals 호출 직전")
                    # 분봉 신호 계산(5분 집계 포함)
                    calc_minute_signals(limit=300)
                    print("✅ calc_minute_signals 호출 완료")

                sleep_sec = 60  # 실행 모드: 1분
            else:
                print(f"🌙 [{now}] 장외 → 대기")
                sleep_sec = 600  # 장외: 10분

            fail_count = 0

        except Exception as e:
            fail_count += 1
            print(f"❌ 루프 에러 ({fail_count}회): {e}")
            traceback.print_exc()
            sleep_sec = min(60 * fail_count, 600)

        time.sleep(sleep_sec)


if __name__ == "__main__":
    main()