from services.infra.db import db_conn
from services.infra.market_time import is_market_open
from services.infra.kis_http import common_headers, kis_get


def get_today_processed_codes():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT code
                FROM candidate_stocks
                WHERE trade_date = CURRENT_DATE
            """)
            return {r[0] for r in cur.fetchall()}


def collect_candidates_by_condition(auth, user_id: str, base_url: str, seq: str, condition_name: str):
    if not is_market_open():
        print(f"⏸️ 장외시간 → [{condition_name}] 스킵")
        return 0

    headers = common_headers(auth, "HHKST03900400")
    res = kis_get(
        base_url,
        "/uapi/domestic-stock/v1/quotations/psearch-result",
        headers=headers,
        params={"user_id": user_id, "seq": seq},
        timeout=5,
    )

    if res.status_code != 200:
        print(f"❌ 조건식 {condition_name} 실행 실패: {res.status_code} {res.text}")
        return 0

    output = res.json().get("output2", [])
    if not output:
        print(f"⚠️ [{condition_name}] 결과 없음")
        return 0

    today_codes = get_today_processed_codes()
    inserted = 0

    with db_conn() as conn:
        with conn.cursor() as cur:
            for item in output:
                code = item["code"]
                if code in today_codes:
                    continue

                cur.execute("""
                    INSERT INTO candidate_stocks (
                        code, name,
                        sources,
                        trade_date,
                        collected_at,
                        updated_at
                    )
                    VALUES (%s, %s, ARRAY[%s], CURRENT_DATE, NOW(), NOW())
                    ON CONFLICT (code) DO UPDATE SET
                        sources = (
                            SELECT ARRAY(
                                SELECT DISTINCT unnest(
                                    candidate_stocks.sources || EXCLUDED.sources
                                )
                            )
                        ),
                        updated_at = NOW()
                """, (code, item["name"], condition_name))
                inserted += 1

    print(f"✅ [{condition_name}] 후보 저장 완료 ({inserted}/{len(output)}건)")
    return inserted
