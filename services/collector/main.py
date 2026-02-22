from services.kis_auth import KISAuth
from services.collector.collector_service import run_once

def main():
    auth = KISAuth()
    target_conditions = {
        "거래대금 상위 + 최소 시총": "4",
        "등락률 상위 + 거래대금": "5",
        "최근 5일 신고가 갱신": "6",
    }
    run_once(auth, auth.user_id, auth.base_url, target_conditions)

if __name__ == "__main__":
    main()
