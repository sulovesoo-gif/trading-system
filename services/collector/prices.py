from services.infra.db import db_conn
from services.infra.kis_http import common_headers, kis_get
from services.collector.common import get_tracked_codes

def update_candidate_prices(auth, base_url: str):
    codes = get_tracked_codes()
    if not codes:
        print("⚠️ candidate_stocks 비어 있음")
        return 0

    headers = common_headers(auth, "FHKST01010100")
    updated = 0

    with db_conn() as conn:
        with conn.cursor() as cur:
            for code in codes:
                res = kis_get(
                    base_url,
                    "/uapi/domestic-stock/v1/quotations/inquire-price",
                    headers=headers,
                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
                    timeout=5
                )

                if res.status_code != 200:
                    print(f"❌ {code} 가격 조회 실패: {res.status_code}")
                    continue

                o = res.json().get("output", {})
                price = int(o.get("stck_prpr", 0))
                rate = float(o.get("prdy_ctrt", 0))
                value = int(o.get("acml_tr_pbmn", 0))

                cur.execute("""
                    UPDATE candidate_stocks
                    SET last_price = %s,
                        change_rate = %s,
                        trade_amount = %s,
                        updated_at = NOW()
                    WHERE code = %s
                """, (price, rate, value, code))
                updated += 1

    print(f"✅ candidate_stocks {updated}/{len(codes)}건 업데이트 완료")
    return updated
