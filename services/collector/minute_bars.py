# services/collector/minute_bars.py
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from services.infra.db import db_conn
from services.infra.market_time import is_market_open
from services.infra.kis_http import common_headers, kis_get
from services.collector.common import get_tracked_codes

KST = ZoneInfo("Asia/Seoul")


def _floor_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _hhmmss(dt: datetime) -> str:
    return dt.strftime("%H%M%S")


def _parse_int(v, default=0) -> int:
    try:
        return int(str(v).replace(",", ""))
    except Exception:
        return default

def _get_last_ts(conn, code: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ts) FROM stock_minute_bars WHERE code=%s", (code,))
        return cur.fetchone()[0]


def _upsert_rows(conn, rows: list[tuple]):
    if not rows:
        return
    sql = """
    INSERT INTO stock_minute_bars
    (code, ts, open_price, high_price, low_price, close_price, volume)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (code, ts) DO UPDATE SET
      open_price  = EXCLUDED.open_price,
      high_price  = EXCLUDED.high_price,
      low_price   = EXCLUDED.low_price,
      close_price = EXCLUDED.close_price,
      volume      = EXCLUDED.volume;
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def _fetch_minute_chunk(auth, base_url: str, code: str, end_hhmmss: str) -> list[dict]:
    """
    KIS '주식당일분봉조회'
    - TR ID: FHKST03010200
    - end_hhmmss(=FID_INPUT_HOUR_1) 기준으로 '이전부터' 최대 30건 반환(당일 데이터)
    """
    headers = common_headers(auth, "FHKST03010200")  # 주식당일분봉조회 :contentReference[oaicite:1]{index=1}
    params = {
        "FID_ETC_CLS_CODE": "",
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_INPUT_HOUR_1": end_hhmmss,      # HHMMSS :contentReference[oaicite:2]{index=2}
        "FID_PW_DATA_INCU_YN": "N",
    }

    res = kis_get(
        base_url,
        "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
        headers=headers,
        params=params,
        timeout=5,
    )

    if res.status_code != 200:
        print(f"❌ [{code}] 분봉 API 실패: {res.status_code} {res.text}")
        return []

    body = res.json()
    # 분봉은 보통 output2에 배열로 내려옴 (샘플/가이드 기준) :contentReference[oaicite:3]{index=3}
    return body.get("output2", []) or []


def _to_ts_kst(item: dict) -> datetime | None:
    """
    output2 원소에서 날짜/시간을 읽어 ts 생성.
    필드명이 환경마다 약간 달라질 수 있어 get으로 흡수.
    """
    date_str = item.get("stck_bsop_date") or item.get("bsop_date") or item.get("trade_date")
    time_str = item.get("stck_cntg_hour") or item.get("cntg_hour") or item.get("stck_hour") or item.get("hour")

    if not date_str or not time_str:
        return None

    # date: YYYYMMDD, time: HHMMSS
    try:
        dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        return dt.replace(tzinfo=KST)
    except Exception:
        return None


def _to_ohlcv(item: dict) -> tuple[int, int, int, int, int]:
    """
    output2 원소에서 OHLCV를 뽑아냄.
    (KIS 응답 필드명이 케이스별로 달라질 수 있어 여러 후보를 흡수)
    """
    o = _parse_int(item.get("stck_oprc") or item.get("open") or item.get("oprc"))
    h = _parse_int(item.get("stck_hgpr") or item.get("high") or item.get("hgpr"))
    l = _parse_int(item.get("stck_lwpr") or item.get("low")  or item.get("lwpr"))
    c = _parse_int(item.get("stck_prpr") or item.get("close") or item.get("prpr") or item.get("stck_clpr"))
    v = _parse_int(item.get("cntg_vol") or item.get("volume") or item.get("acml_vol"))
    return o, h, l, c, v


def collect_minute_bars_once(auth, base_url: str, max_codes: int | None = None) -> int:
    """
    - 장중에만 수행
    - 각 종목별로 '마지막 저장 ts' 이후부터 현재까지 누락을 메움
    - API가 1회 최대 30건이어서, 필요하면 윈도우를 과거로 이동하며 여러 번 호출
    """
    if not is_market_open():
        print("⏸️ 장외시간 → [1분봉 수집] 스킵")
        return 0

    codes = get_tracked_codes()
    if max_codes:
        codes = codes[:max_codes]

    now = _floor_to_minute(datetime.now(KST))

    total_saved = 0

    with db_conn() as conn:
        for code in codes:
            last_ts = _get_last_ts(conn, code)
            # 처음이면 오늘 09:00부터(원하면 정책 바꿔도 됨)
            if last_ts is None:
                start_ts = now.replace(hour=9, minute=0)
            else:
                start_ts = _floor_to_minute(last_ts.astimezone(KST) + timedelta(minutes=1))

            if start_ts > now:
                continue

            # end_cursor는 '현재'에서 시작해서 과거로 30개씩 당겨오며 start_ts까지 메움
            end_cursor = now
            inserted_this_code = 0

            while True:
                chunk = _fetch_minute_chunk(auth, base_url, code, _hhmmss(end_cursor))
                if not chunk:
                    break

                rows = []
                min_ts_in_chunk = None
                max_ts_in_chunk = None

                for item in chunk:
                    ts = _to_ts_kst(item)
                    if ts is None:
                        continue

                    ts = _floor_to_minute(ts)

                    # 필요한 구간만
                    if ts < start_ts or ts > now:
                        continue

                    o, h, l, c, v = _to_ohlcv(item)
                    rows.append((code, ts, o, h, l, c, v))

                    min_ts_in_chunk = ts if (min_ts_in_chunk is None or ts < min_ts_in_chunk) else min_ts_in_chunk
                    max_ts_in_chunk = ts if (max_ts_in_chunk is None or ts > max_ts_in_chunk) else max_ts_in_chunk

                if rows:
                    _upsert_rows(conn, rows)
                    inserted_this_code += len(rows)
                    total_saved += len(rows)

                # 이 chunk가 start_ts 이전까지 커버하면 종료
                if min_ts_in_chunk is None:
                    break
                if min_ts_in_chunk <= start_ts:
                    break

                # 더 과거로: chunk의 최소 ts보다 1분 더 과거를 end_cursor로 잡고 반복
                end_cursor = _floor_to_minute(min_ts_in_chunk - timedelta(minutes=1))

            if inserted_this_code:
                print(f"✅ [{code}] 1분봉 저장 {inserted_this_code}건")

    print(f"✅ stock_minute_bars 저장 합계 {total_saved}건")
    return total_saved
