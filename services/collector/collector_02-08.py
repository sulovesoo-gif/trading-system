import time
import requests
import os
import psycopg2
from datetime import datetime
from bs4 import BeautifulSoup
from kis_auth import KISAuth

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

    def _get_db_connection(self):
        return psycopg2.connect(**self.db_params)

    def _get_common_headers(self, tr_id):
        return {"""  """
            "Content-Type" : "application/json",
            "authorization": f"Bearer {self.auth.get_access_token()}",
            "appkey"       : self.api_key,
            "appsecret"    : self.api_secret,
            "tr_id"        : tr_id,
            "custtype"     : "P"
        }

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
        data = res.json()
        print("keys:", data.keys())
        print("sample:", data.get("output", [])[:3])

    if __name__ == "__main__":
        c = StockCollector()
        c.test_volume_rank()























    # 한국투자증권에 내가 만든 조건 목록
    # 목록에서 Seq를 가져오는
    def stocks_by_condition(self):
        token   = self.auth.get_access_token()
        headers = self._get_common_headers("HHKST03900300")
        try:
            res = requests.get(f"{self.base_url}/uapi/domestic-stock/v1/quotations/psearch-title", headers=headers, params={"user_id": self.user_id})
            if res.status_code == 200:
                output = res.json().get('output2', [])
                self.condition_map = {item['condition_nm'].strip(): item['seq'] for item in output}
                print(f"? HTS 조건식 {len(self.condition_map)}개 로드 완료")
        except Exception as e: print(f"? 목록 로드 실패: {e}")
    
    # 한국투자증권에 내가 만든 조건 검색
    def collect_candidate_stocks(self):
        token   = self.auth.get_access_token()
        headers = self._get_common_headers("HHKST03900400")
        try:
            res = requests.get(f"{self.base_url}/uapi/domestic-stock/v1/quotations/psearch-title", headers=headers, params={"user_id": self.user_id})
            if res.status_code == 200:
                output = res.json().get('output2', [])
                self.condition_map = {item['condition_nm'].strip(): item['seq'] for item in output}
                print(f"? HTS 조건식 {len(self.condition_map)}개 로드 완료")
        except Exception as e: print(f"? 목록 로드 실패: {e}")

    def fetch_kis_rank_data(self):
        token   = self.auth.get_access_token()
        headers = self._get_common_headers("HHKST03900300")
        params  = {
            "FID_COND_MRKT_DIV_CODE": "J", 
            "FID_COND_SCR_DIV_CODE" : "20171",
            "FID_INPUT_ISCD"        : "0000", 
            "FID_DIV_CLS_CODE"      : "0", 
            "FID_BLNG_CLS_CODE"     : "0",
            "FID_TR_GT_LS_CLS_CODE" : "0", 
            "FID_VOL_CNTN"          : "0", 
            "FID_INPUT_DATE_1"      : "",
            "FID_RANK_SORT_CLS_CODE": "3"
        }
        try:
            res = requests.get(f"{self.base_url}/uapi/domestic-stock/v1/quotations/volume-rank", headers=headers, params=params)
            master_dict = {}
            if res.status_code == 200:
                for item in res.json().get('output', []):
                    sym = item.get('mksc_shrn_iscd')
                    master_dict[sym] = {
                        'name'       : item.get('hts_kor_isnm'),
                        'price'      : float(item.get('stck_prpr', 0)),
                        'profit'     : float(item.get('prdy_ctrt', 0)),
                        'value'      : int(item.get('acml_tr_pbmn', 0)),
                        'vol'        : int(item.get('acml_vol', 0)),
                        'prev_vol'   : int(item.get('prdy_vol', 0)),
                        'is_rank_100': int(item.get('data_rank', 999)) <= 100
                    }
            return master_dict
        except: return {}

    def get_stocks_by_condition(self, seq):
        token = self.auth.get_access_token()
        headers = {
            "Content-Type": "application/json", "authorization": f"Bearer {token}",
            "appkey": self.api_key, "appsecret": self.api_secret,
            "tr_id": "HHKST03900400", "custtype": "P"
        }
        try:
            url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/psearch-result"
            res = requests.get(url, headers=headers, params={"user_id": self.user_id, "seq": seq})
            return [item['code'] for item in res.json().get('output2', [])] if res.status_code == 200 else []
        except: return []

    def fetch_kis_rank_data(self):
        token = self.auth.get_access_token()
        headers = {
            "Content-Type": "application/json", "authorization": f"Bearer {token}",
            "appkey": self.api_key, "appsecret": self.api_secret,
            "tr_id": "FHPST01710000", "custtype": "P"
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "0", "FID_BLNG_CLS_CODE": "0",
            "FID_TR_GT_LS_CLS_CODE": "0", "FID_VOL_CNTN": "0", "FID_INPUT_DATE_1": "",
            "FID_RANK_SORT_CLS_CODE": "3"
        }
        try:
            res = requests.get(f"{self.base_url}/uapi/domestic-stock/v1/quotations/volume-rank", headers=headers, params=params)
            master_dict = {}
            if res.status_code == 200:
                for item in res.json().get('output', []):
                    sym = item.get('mksc_shrn_iscd')
                    master_dict[sym] = {
                        'name': item.get('hts_kor_isnm'),
                        'price': float(item.get('stck_prpr', 0)),
                        'profit': float(item.get('prdy_ctrt', 0)),
                        'value': int(item.get('acml_tr_pbmn', 0)),
                        'vol': int(item.get('acml_vol', 0)),
                        'prev_vol': int(item.get('prdy_vol', 0)),
                        'is_rank_100': int(item.get('data_rank', 999)) <= 100
                    }
            return master_dict
        except: return {}














    def collect_naver_themes(self):
        """네이버 테마 마스터 갱신"""
        print(f"?? [{datetime.now().strftime('%H:%M:%S')}] 네이버 테마 갱신 시작...")
        url = "https://finance.naver.com/sise/theme.naver"
        try:
            res = requests.get(url, timeout=5)
            res.encoding = 'euc-kr'
            soup = BeautifulSoup(res.text, 'html.parser')
            theme_rows = soup.select('.type_1 tr')[3:13]
            for row in theme_rows:
                cols = row.select('td')
                if not cols or not cols[0].find('a'): continue
                theme_name = cols[0].get_text(strip=True)
                theme_link = "https://finance.naver.com" + cols[0].find('a')['href']
                self._scrape_theme_details(theme_name, theme_link)
        except Exception as e:
            print(f"? 네이버 테마 수집 에러: {e}")

    def _scrape_theme_details(self, theme_name, url):
        try:
            res = requests.get(url, timeout=5)
            res.encoding = 'euc-kr'
            soup = BeautifulSoup(res.text, 'html.parser')
            stock_rows = soup.select('.type_5 tr')
            for row in stock_rows:
                name_td = row.select_one('.name a')
                if name_td and 'href' in name_td.attrs:
                    symbol = name_td['href'].split('=')[-1]
                    self._save_theme_to_db(symbol, theme_name, "NAVER_THEME")
        except Exception: pass

    def _save_theme_to_db(self, symbol, theme_name, source):
        try:
            with self._get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO theme_stocks (symbol, theme_name, source, start_date, updated_at)
                        VALUES (%s, %s, %s, CURRENT_DATE, NOW())
                        ON CONFLICT (symbol, theme_name) 
                        DO UPDATE SET updated_at = NOW(), end_date = NULL;
                    """, (symbol, theme_name, source))
                conn.commit()
        except Exception: pass

    def update_tracked_stocks(self, info_dict):
        """공용: 기존 포착 종목의 실시간 수익률 계산 및 업데이트"""
        try:
            with self._get_db_connection() as conn:
                with conn.cursor() as cur:
                    for symbol, data in info_dict.items():
                        # 진입가 대비 수익률 계산 (ROUND 함수 사용)
                        cur.execute("""
                            UPDATE detected_signals 
                            SET current_price = %s,
                                profit_rate = ROUND(((%s::numeric - entry_price) / entry_price) * 100, 2)
                            WHERE symbol = %s AND CAST(timestamp AS date) = CURRENT_DATE;
                        """, (data['price'], data['price'], symbol))
                conn.commit()
        except Exception as e:
            print(f"? 수익률 업데이트 실패: {e}")

    def detect_market_leaders(self):
        """주도주 검색 및 저장"""
        print(f"?? [{datetime.now().strftime('%H:%M:%S')}] 주도주 분석 및 수익률 갱신...")
        headers = self._get_common_headers("FHPST01710000")
        url_rank = f"{self.base_url}/uapi/domestic-stock/v1/quotations/volume-rank"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": "0", "FID_TR_GT_LS_CLS_CODE": "0",
            "FID_VOL_CNTN": "0", "FID_INPUT_DATE_1": ""
        }
        try:
            res = requests.get(url_rank, headers=headers, params=params).json()
            output = res.get('output', [])
            
            if not output: # 테스트용 가상 데이터
                output = [
                    {'mksc_shrn_iscd': '005930', 'hts_kor_isnm': '삼성전자', 'stck_prpr': '73500'},
                    {'mksc_shrn_iscd': '000660', 'hts_kor_isnm': 'SK하이닉스', 'stck_prpr': '141000'}
                ]

            info_dict = {
                item['mksc_shrn_iscd']: {
                    'name': item['hts_kor_isnm'], 
                    'price': int(item.get('stck_prpr', 0))
                } for item in output[:20]
            }
            
            # 1. 신규 종목 저장 (최초 포착가 기록)
            self._save_signals(list(info_dict.keys()), "LEADER", info_dict)
            
            # 2. 기존 종목 수익률 실시간 업데이트 (요청하신 로직 추가)
            self.update_tracked_stocks(info_dict)
            
        except Exception as e:
            print(f"? 수집 에러: {e}")

    def _save_signals(self, symbols, signal_type, info_dict):
        try:
            with self._get_db_connection() as conn:
                with conn.cursor() as cur:
                    for symbol in symbols:
                        # ON CONFLICT DO NOTHING을 통해 최초 포착가(entry_price)를 보존함
                        cur.execute("""
                            INSERT INTO detected_signals (symbol, stock_name, signal_type, entry_price, current_price, timestamp)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (symbol, signal_type, (CAST(timestamp AS date))) DO NOTHING;
                        """, (symbol, info_dict[symbol]['name'], signal_type, info_dict[symbol]['price'], info_dict[symbol]['price']))
                conn.commit()
            print(f"? [{signal_type}] 신규 포착 처리 완료")
        except Exception as e:
            print(f"? DB 저장 에러: {e}")

    def run(self):
        while True:
            try:
                self.collect_naver_themes()
                self.detect_market_leaders()
                print("?? 1분 대기 중...")
                time.sleep(60)
            except Exception as e:
                print(f"?? 루프 에러: {e}")
                time.sleep(10)

if __name__ == "__main__":
    collector = StockCollector()
    collector.run()