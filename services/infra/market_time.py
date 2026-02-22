from datetime import datetime, time
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

def is_market_open(now: datetime | None = None) -> bool:
    now = now or datetime.now(KST)
    t = now.timetz()  # KST time
    # 한국장: 09:00~15:30 (필요시 요일/공휴일 로직은 기존 그대로 유지)
    return time(9, 0) <= t.replace(tzinfo=None) <= time(15, 30)

# def is_market_open(now=None) -> bool:
#     now = (now or datetime.now()).time()
#     open_t = datetime.strptime("09:00", "%H:%M").time()
#     close_t = datetime.strptime("15:30", "%H:%M").time()
#     return open_t <= now <= close_t
