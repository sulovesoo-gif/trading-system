from datetime import datetime


def is_market_open(now=None) -> bool:
    now = (now or datetime.now()).time()
    open_t = datetime.strptime("09:00", "%H:%M").time()
    close_t = datetime.strptime("15:30", "%H:%M").time()
    return open_t <= now <= close_t
