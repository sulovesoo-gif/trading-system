import os
import time
import requests
from typing import Optional, Dict, Tuple

# (code, signal_type) -> last_sent_epoch
_LAST_SENT: Dict[Tuple[str, str], float] = {}

def send_telegram(message: str) -> bool:
    TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "8278913301:AAGgous2CAcYKAf7L_hvJOEmXOErNfNPUTw")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "8389619558")
    print(TELEGRAM_TOKEN)
    print(TELEGRAM_CHAT_ID)
    print(message)
    """텔레그램 메시지 전송. env 없으면 조용히 무시."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ TELEGRAM_TOKEN/CHAT_ID 비어있음")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, data=data, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

def should_send(code: str, signal_type: str, cooldown_sec: int = 180) -> bool:
    """
    종목/시그널 타입별 쿨다운.
    예) 동일 종목 BUY 신호가 3분 안에 또 발생하면 알림 스킵.
    """
    now = time.time()
    key = (code, signal_type)
    last = _LAST_SENT.get(key, 0.0)
    if now - last < cooldown_sec:
        return False
    _LAST_SENT[key] = now
    return True

def fmt_num(x: Optional[float], nd: int = 2) -> str:
    if x is None:
        return "-"
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return str(x)

def fmt_int(x: Optional[int]) -> str:
    if x is None:
        return "-"
    try:
        return f"{int(x):,}"
    except Exception:
        return str(x)
