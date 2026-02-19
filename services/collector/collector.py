import time
import requests
import os
import psycopg2
import numpy as np
import pandas as pd
from scipy.stats import linregress
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from kis_auth import KISAuth

PERIOD = 14
BB_PERIOD = 20
BB_STD = 2

class StockCollector:
    def __init__(self):
        self.auth       = KISAuth()
        self.api_key    = self.auth.api_key
        self.api_secret = self.auth.api_secret
        self.user_id    = self.auth.user_id
        self.base_url   = self.auth.base_url
        self.db_params  = {
            'host'    : os.getenv('DB_HOST'          , 'localhost'),
            'database': os.getenv('POSTGRES_DB'      , 'trading_db'),
            'user'    : os.getenv('POSTGRES_USER'    , 'admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'stock123'),
            'port'    : 5432
        }
        self.condition_map    = {}
        self.target_cond_seqs = {
            'cap_time_6': '0', 
            'cap_time_7': '1', 
            'cap_time_8': '2', 
            'cap_time_9': '3'
        }
        self.target_conditions = {
            "거래대금 상위 + 최소 시총": "4",
            "등락률 상위 + 거래대금": "5",
            "최근 5일 신고가 갱신": "6"
        }

    def _get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def _get_common_headers(self, tr_id):
        return {
            "Content-Type" : "application/json",
            "authorization": f"Bearer {self.auth.get_access_token()}",
            "appkey"       : self.api_key,
            "appsecret"    : self.api_secret,
            "tr_id"        : tr_id,
            "custtype"     : "P"
        }

    def is_market_open(self):
        now = datetime.now().time()
        return now >= datetime.strptime("09:00", "%H:%M").time() \
            and now <= datetime.strptime("15:30", "%H:%M").time()

    def get_today_processed_codes(self):
        with self._get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT code
                    FROM candidate_stocks
                    WHERE trade_date = CURRENT_DATE
                """)
                return {r[0] for r in cur.fetchall()}

    def collect_candidates_by_condition(self, seq: str, condition_name: str):
        if not self.is_market_open():
            print(f"⏸️ 장외시간 → [{condition_name}] 스킵")
            return

        headers = self._get_common_headers("HHKST03900400")

        res = requests.get(
            f"{self.base_url}/uapi/domestic-stock/v1/quotations/psearch-result",
            headers=headers,
            params={"user_id": self.user_id, "seq": seq}
        )

        if res.status_code != 200:
            print(f"❌ 조건식 {condition_name} 실행 실패")
            return

        output = res.json().get("output2", [])
        if not output:
            print(f"⚠️ [{condition_name}] 결과 없음")
            return

        today_codes = self.get_today_processed_codes()

        with self._get_db_connection() as conn:
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
                    """, (
                        code,
                        item["name"],
                        condition_name
                    ))

            conn.commit()

        print(f"✅ [{condition_name}] 후보 저장 완료 ({len(output)}건)")

    def load_my_conditions(self):
        headers = self._get_common_headers("HHKST03900300")

        res = requests.get(
            f"{self.base_url}/uapi/domestic-stock/v1/quotations/psearch-title",
            headers=headers,
            params={"user_id": self.user_id}
        )

        if res.status_code != 200:
            print("❌ 조건식 목록 조회 실패", res.text)
            return

        output = res.json().get("output2", [])
        if not output:
            print("⚠️ 조건식 없음 (HTS 서버저장 확인)")
            return

        self.condition_map = {
            item["condition_nm"]: item["seq"]
            for item in output
        }

        print("✅ 조건식 로드 완료")
        for name, seq in self.condition_map.items():
            print(f"  - {name} → seq={seq}")

    def update_candidate_prices(self):
        # 1️⃣ 후보 종목 조회
        with self._get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT code FROM candidate_stocks")
                codes = [r[0] for r in cur.fetchall()]

        if not codes:
            print("⚠️ candidate_stocks 비어 있음")
            return

        headers = self._get_common_headers("FHKST01010100")

        # 2️⃣ 업데이트용 커넥션은 한 번만
        with self._get_db_connection() as conn:
            with conn.cursor() as cur:
                for code in codes:
                    params = {
                        "FID_COND_MRKT_DIV_CODE": "J",
                        "FID_INPUT_ISCD": code
                    }

                    res = requests.get(
                        f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                        headers=headers,
                        params=params
                    )

                    if res.status_code != 200:
                        print(f"❌ {code} 가격 조회 실패")
                        continue

                    o = res.json().get("output", {})
                    price = int(o.get("stck_prpr", 0))
                    rate  = float(o.get("prdy_ctrt", 0))
                    value = int(o.get("acml_tr_pbmn", 0))

                    cur.execute("""
                        UPDATE candidate_stocks
                        SET last_price = %s,
                            change_rate = %s,
                            trade_amount = %s,
                            updated_at = NOW()
                        WHERE code = %s
                    """, (price, rate, value, code))

                conn.commit()

        print(f"✅ candidate_stocks {len(codes)}건 업데이트 완료")

    def collect_all_daily(self):
        with self._get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT code FROM candidate_stocks")
                codes = [r[0] for r in cur.fetchall()]

        for code in codes:
            self.collect_daily_candles(code)
            time.sleep(0.2)
    
    def collect_daily_candles(self, code: str):
        headers = self._get_common_headers("FHKST01010400")

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_PERIOD_DIV_CODE": "D",  # 일봉 (최근 30일)
            "FID_ORG_ADJ_PRC": "1"       # 수정주가
        }

        res = requests.get(
            f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers=headers,
            params=params,
            timeout=5
        )

        if res.status_code != 200:
            print(f"❌ [{code}] 일봉 API 실패:", res.text)
            return

        output = res.json().get("output", [])
        if not output:
            print(f"⚠️ [{code}] 일봉 데이터 없음")
            return

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
                None  # trade_amount 없음 → NULL
            ))

        with self._get_db_connection() as conn:
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
            conn.commit()

        print(f"✅ [{code}] 일봉 {len(rows)}건 저장 완료")

    def calculate_daily_indicators(self):
        with self._get_db_connection() as conn:
            df = pd.read_sql("""
                SELECT
                    code,
                    trade_date,
                    close_price,
                    volume
                FROM stock_daily_bars
                ORDER BY code, trade_date
            """, conn)

        if df.empty:
            print("⚠️ 일봉 데이터 없음")
            return

        results = []

        for code, g in df.groupby("code"):
            g = g.reset_index(drop=True)

            if len(g) < BB_PERIOD:
                continue

            # === 이동평균 ===
            g["ma"] = g["close_price"].rolling(PERIOD).mean()

            # === 거래량 ===
            g["volume_avg"] = g["volume"].rolling(PERIOD).mean()
            g["volume_spike"] = g["volume"] / g["volume_avg"]

            # === 볼린저 밴드 ===
            g["bb_mid"] = g["close_price"].rolling(BB_PERIOD).mean()
            g["bb_std"] = g["close_price"].rolling(BB_PERIOD).std()
            g["bb_upper"] = g["bb_mid"] + BB_STD * g["bb_std"]
            g["bb_lower"] = g["bb_mid"] - BB_STD * g["bb_std"]

            # === 최근 period 기준 회귀 ===
            sub = g.iloc[-PERIOD:]
            x = np.arange(PERIOD)
            y = sub["close_price"].values

            slope, intercept, r_value, _, _ = linregress(x, y)
            lrl_value = intercept + slope * (PERIOD - 1)
            r_square = r_value ** 2

            last = g.iloc[-1]
            prev = g.iloc[-2]

            results.append((
                code,
                datetime.combine(last["trade_date"], datetime.min.time()),
                PERIOD,
                float(last["ma"]),
                float(lrl_value),
                float(slope),
                float(r_square),
                float(last["bb_upper"]),
                float(last["bb_lower"]),
                float(last["volume_avg"]),
                float(last["volume_spike"]),
                last["close_price"] >= last["bb_upper"],
                prev["close_price"] < prev["bb_upper"] and last["close_price"] >= last["bb_upper"],
                last["close_price"] <= last["bb_lower"],
                prev["close_price"] > prev["bb_lower"] and last["close_price"] <= last["bb_lower"],
            ))

        if not results:
            print("⚠️ 계산 결과 없음")
            return

        with self._get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO stock_indicators (
                        code, ts, period,
                        ma, lrl_value, lrl_slope, r_square,
                        bb_upper, bb_lower,
                        volume_avg, volume_spike,
                        bb_upper_touch, bb_upper_break,
                        bb_lower_touch, bb_lower_break
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (code, ts, period) DO UPDATE SET
                        ma = EXCLUDED.ma,
                        lrl_value = EXCLUDED.lrl_value,
                        lrl_slope = EXCLUDED.lrl_slope,
                        r_square = EXCLUDED.r_square,
                        bb_upper = EXCLUDED.bb_upper,
                        bb_lower = EXCLUDED.bb_lower,
                        volume_avg = EXCLUDED.volume_avg,
                        volume_spike = EXCLUDED.volume_spike,
                        bb_upper_touch = EXCLUDED.bb_upper_touch,
                        bb_upper_break = EXCLUDED.bb_upper_break,
                        bb_lower_touch = EXCLUDED.bb_lower_touch,
                        bb_lower_break = EXCLUDED.bb_lower_break,
                        created_at = NOW()
                """, results)
            conn.commit()

        print(f"✅ stock_indicators {len(results)}건 계산 완료")
    
    def collect_daily_candles2(self, code: str, days: int = 100):
        # 특정 종목(code)의 일봉 데이터를 조회하여 stock_daily_bars 테이블에 저장
        headers = self._get_common_headers("FHKST01010400")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",     # 주식
            "FID_INPUT_ISCD": code,            # 종목코드
            "FID_PERIOD_DIV_CODE": "D",        # 일봉
            "FID_ORG_ADJ_PRC": "1"             # 수정주가
        }

        try:
            res = requests.get(
                f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                headers=headers,
                params=params,
                timeout=5
            )

            if res.status_code != 200:
                print(f"❌ [{code}] 일봉 API 실패:", res.text)
                return

            output = res.json().get("output2", [])
            if not output:
                print(f"⚠️ [{code}] 일봉 데이터 없음")
                return

            rows = []
            for item in output[:days]:
                rows.append((
                    code,
                    item.get("stck_bsop_date"),     # 거래일
                    int(item.get("stck_oprc", 0)),  # 시가
                    int(item.get("stck_hgpr", 0)),  # 고가
                    int(item.get("stck_lwpr", 0)),  # 저가
                    int(item.get("stck_clpr", 0)),  # 종가
                    int(item.get("acml_vol", 0)),   # 거래량
                    int(item.get("acml_tr_pbmn", 0))# 거래대금
                ))

            with self._get_db_connection() as conn:
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
                conn.commit()

            print(f"✅ [{code}] 일봉 {len(rows)}건 저장 완료")

        except Exception as e:
            print(f"❌ [{code}] 일봉 수집 에러:", e)
    
    def test_volume_rank(self):
        headers = self._get_common_headers("FHPST01710000")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000",
            "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": "0",
            "FID_TR_GT_LS_CLS_CODE": "0",
            "FID_VOL_CNTN": "0",
            "FID_INPUT_DATE_1": "",
            "FID_RANK_SORT_CLS_CODE": "3"
        }

        res = requests.get(
            f"{self.base_url}/uapi/domestic-stock/v1/quotations/volume-rank",
            headers=headers,
            params=params
        )

        print("status:", res.status_code)

        #output = res.json().get('Output', [])
        #print("keys:", output)
        data = res.json()

        print("keys:", data.keys())
        print("msg1:", data.get("msg1"))
        print("sample:", data.get("output", [])[:3])

if __name__ == "__main__":
    c = StockCollector()

    # 1️⃣ 조건식 목록 조회
    # c.load_my_conditions()

    # 2️⃣ 사용할 조건식만 실행 (4,5,6)


    for name, seq in c.target_conditions.items():
        c.collect_candidates_by_condition(seq, name)

    # 3️⃣ 후처리
    c.update_candidate_prices()
    c.collect_all_daily()