import time
from services.infra.db import db_conn
from services.infra.kis_http import common_headers, kis_get
from services.collector.common import get_tracked_codes

def collect_daily_candles(auth, base_url: str, code: str):
    headers = common_headers(auth, "FHKST01010400")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_PERIOD_DIV_CODE": "D",
        "FID_ORG_ADJ_PRC": "1"
    }

    try:
        res = kis_get(
            base_url,
            "/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers=headers,
            params=params,
            timeout=60,
        )
    except Exception as e:
        print(f"❌ {code} 일봉 조회 예외(스킵): {e}")
        return 0   # ✅ continue → return으로 변경

    if res.status_code != 200:
        print(f"❌ [{code}] 일봉 API 실패: {res.status_code} {res.text}")
        return 0

    output = res.json().get("output", [])
    if not output:
        print(f"⚠️ [{code}] 일봉 데이터 없음")
        return 0

    rows = []
    for item in output:
        rows.append((
            code,
            item["stck_bsop_date"],
            int(item["stck_oprc"]),
            int(item["stck_hgpr"]),
            int(item["stck_lwpr"]),
            int(item["stck_clpr"]),
            int(item["acml_vol"]),
            None
        ))

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO stock_daily_bars (
                    code, trade_date,
                    open_price, high_price, low_price, close_price,
                    volume, trade_amount
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (code, trade_date) DO NOTHING
            """, rows)

    print(f"✅ [{code}] 일봉 {len(rows)}건 저장 완료")
    return len(rows)


def collect_all_daily(auth, base_url: str, sleep_sec: float = 0.2):
    codes = get_tracked_codes()
    total = 0
    for code in codes:
        total += collect_daily_candles(auth, base_url, code)
        time.sleep(sleep_sec)
    return total
