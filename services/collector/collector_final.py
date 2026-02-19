import time
import re
import requests
import os
import psycopg2
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from kis_auth import KISAuth
from dotenv import load_dotenv

load_dotenv()

class StockCollector:
    def __init__(self):
        self.auth = KISAuth()
        self.api_key = self.auth.api_key
        self.api_secret = self.auth.api_secret
        self.user_id = self.auth.user_id
        self.base_url = self.auth.base_url
        
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'trading_db'),
            'user': os.getenv('POSTGRES_USER', 'admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'stock123'),
            'port': 5432
        }
        
        self.condition_map = {}
        self.target_cond_seqs = {
            'cap_time_6': '0', 
            'cap_time_7': '1', 
            'cap_time_8': '2', 
            'cap_time_9': '3'
        }

    def _get_db_connection(self):
        try:
            conn = psycopg2.connect(**self.db_params)
            conn.set_client_encoding('UTF8') 
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'Asia/Seoul';")
            return conn
        except Exception as e:
            print(f"? DB 연결 실패: {e}")
            return None

    ###########################################################################
    # --- [기획안 3-①: 네이버 테마, 뉴스 및 생애주기 통합 로직] ---
    ###########################################################################
    def fetch_naver_data(self):
        """최근 포착된 종목 대상으로 테마 및 뉴스 동기화"""
        conn = self._get_db_connection()
        if not conn: return
        
        try:
            with conn.cursor() as cur:
                # 1. 최근 24시간 내 신호가 포착된 종목 추출 (기획안 핵심)
                cur.execute("""
                    SELECT symbol FROM detected_signals 
                    WHERE updated_at >= NOW() - INTERVAL '24 hours'
                """)
                target_symbols = [row[0] for row in cur.fetchall()]
            
            if not target_symbols:
                print("ℹ️ 최근 24시간 내 포착 종목 없음. (스킵)")
                return

            print(f"🔍 [Naver Sync] {len(target_symbols)}개 종목 분석 시작...")
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            
            for symbol in target_symbols:
                try:
                    url = f"https://finance.naver.com/item/main.naver?code={symbol}"
                    res = requests.get(url, headers=headers, timeout=5)
                    html = res.text
                    soup = BeautifulSoup(html, 'html.parser')

                    # 종목명 확인 및 마스터 업데이트
                    name_tag = soup.select_one('div.wrap_company > h2 > a')
                    stock_name = name_tag.get_text(strip=True) if name_tag else "알수없음"
                    self._sync_stock_info(symbol, stock_name)

                    # A. 테마 파싱 및 저장 (정규식 사용)
                    # 네이버 테마 영역: <a href="/sise/sise_group_detail.naver?type=theme&no=...">테마명</a>
                    themes = re.findall(r'no=(\d+)[^>]*>([^<]+)</a>', html)
                    current_themes = []
                    
                    for _, t_name in set(themes):
                        t_name = t_name.strip()
                        # 무의미한 키워드 필터링
                        if len(t_name) > 1 and t_name not in ['더보기', '테마별', '종목토론실']:
                            self._save_theme_to_db(symbol, t_name)
                            current_themes.append(t_name)
                    
                    # B. 생애주기 관리 (사라진 테마 end_date 처리)
                    self.update_theme_lifecycle(symbol, current_themes)

                    # C. 관련 뉴스 추출 및 저장 (추가 기획)
                    self._extract_and_save_news(symbol, soup)
                    
                    print(f"✅ {stock_name}({symbol}): [테마 {len(current_themes)}개] [뉴스 동기화 완료]")
                    time.sleep(0.1) # 서버 부하 방지
                    
                except Exception as e:
                    print(f"⚠️ {symbol} 처리 중 오류: {e}")
        finally:
            conn.close()

    def _save_theme_to_db(self, symbol, theme_name):
        """theme_stocks 테이블에 테마 정보 저장 (ON CONFLICT 적용)"""
        conn = self._get_db_connection()
        if not conn: return
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO theme_stocks (symbol, theme_name, source, start_date, updated_at, end_date)
                        VALUES (%s, %s, 'NAVER', CURRENT_DATE, NOW(), '9999-12-31')
                        ON CONFLICT (symbol, theme_name) 
                        DO UPDATE SET updated_at = NOW(), end_date = '9999-12-31';
                    """, (symbol, theme_name))
        finally: conn.close()

    def update_theme_lifecycle(self, symbol, current_themes):
        """DB에는 활성(9999-12-31)인데 현재 페이지에 없는 테마는 닫기"""
        conn = self._get_db_connection()
        if not conn: return
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT theme_name FROM theme_stocks 
                        WHERE symbol = %s AND end_date = '9999-12-31'
                    """, (symbol,))
                    db_themes = [row[0] for row in cur.fetchall()]
                    
                    for d_theme in db_themes:
                        if d_theme not in current_themes:
                            cur.execute("""
                                UPDATE theme_stocks SET end_date = CURRENT_DATE, updated_at = NOW()
                                WHERE symbol = %s AND theme_name = %s AND end_date = '9999-12-31'
                            """, (symbol, d_theme))
                            print(f"📉 [Theme Out] {symbol}: '{d_theme}' 테마 종료")
        finally: conn.close()

    def _extract_and_save_news(self, symbol, soup):
        """네이버 종목 상세 페이지 하단 최신 뉴스 섹션 추출"""
        # 뉴스 섹션 셀렉터 (네이버 금융 구조 기준)
        news_items = soup.select('div.news_section ul li')
        conn = self._get_db_connection()
        if not conn: return
        
        try:
            with conn:
                with conn.cursor() as cur:
                    for item in news_items:
                        # 1. 제목 및 링크 추출
                        a_tag = item.select_one('a')
                        if not a_tag: continue
                        
                        title = a_tag.get_text(strip=True)
                        link = a_tag.get('href', '')
                        # 상대 경로일 경우 절대 경로로 변경
                        if link.startswith('/'): link = "https://finance.naver.com" + link
                        
                        # 2. 신문사(출처) 추출 
                        # 보통 <span> 혹은 클래스명 'press' 등으로 존재
                        source = ""
                        press_tag = item.select_one('.press')
                        if press_tag:
                            source = press_tag.get_text(strip=True)
                            
                        # 3. 작성 시간 추출
                        # 보통 <span> 혹은 클래스명 'wdate' 등으로 존재
                        pub_date_str = None
                        wdate_tag = item.select_one('.wdate')
                        if wdate_tag:
                            pub_date_str = wdate_tag.get_text(strip=True)
                            
                        # 4. 뉴스 테이블에 저장 (중복 링크 방지)
                        cur.execute("""
                            INSERT INTO stock_news (symbol, title, link, source, pub_date, created_at)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (link) DO NOTHING
                        """, (symbol, title, link, source, pub_date_str))
        finally: conn.close()

    def _sync_stock_info(self, symbol, stock_name):
        conn = self._get_db_connection()
        if not conn: return
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO stock_master (symbol, stock_name, updated_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (symbol) DO UPDATE SET stock_name = EXCLUDED.stock_name, updated_at = NOW();
                    """, (symbol, stock_name))
        finally: conn.close()
    ###########################################################################
    # --- [네이버 크롤링 로직 끝] ---
    ###########################################################################

    def fetch_hts_condition_list(self):
        token = self.auth.get_access_token()
        headers = {
            "Content-Type": "application/json", "authorization": f"Bearer {token}",
            "appkey": self.api_key, "appsecret": self.api_secret,
            "tr_id": "HHKST03900300", "custtype": "P"
        }
        try:
            url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/psearch-title"
            res = requests.get(url, headers=headers, params={"user_id": self.user_id})
            if res.status_code == 200:
                output = res.json().get('output2', [])
                self.condition_map = {item['condition_nm'].strip(): item['seq'] for item in output}
                print(f"? HTS 조건식 {len(self.condition_map)}개 로드 완료")
        except Exception as e: print(f"? 목록 로드 실패: {e}")

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

    def process_and_save(self, master_dict, hts_results):
        conn = self._get_db_connection()
        if not conn: return
        try:
            with conn:
                with conn.cursor() as cur:
                    now = datetime.now()
                    all_targets = set(master_dict.keys())
                    for stocks in hts_results.values(): all_targets.update(stocks)

                    for sym in all_targets:
                        # --- [네이버 데이터 활용 시작] ---
                        # stock_master 테이블에 테마와 뉴스를 함께 업데이트하기 위해 크롤링 호출
                        n_name, n_theme, n_news = self.fetch_naver_data(sym)
                        
                        data = master_dict.get(sym)
                        final_name = n_name if n_name != "알수없음" else (data['name'] if data else "HTS검출")
                        
                        # 1. stock_master 등록 (네이버 테마, 뉴스 정보 포함)
                        cur.execute("""
                            INSERT INTO stock_master (symbol, stock_name, recent_news, updated_at) 
                            VALUES (%s, %s, %s, %s, NOW()) 
                            ON CONFLICT (symbol) DO UPDATE SET 
                                stock_name = EXCLUDED.stock_name,
                                theme_name = EXCLUDED.theme_name,
                                recent_news = EXCLUDED.recent_news,
                                updated_at = NOW();
                        """, (sym, final_name, n_news))
                        
                        # 2. theme_stocks 등록 (네이버 테마, 뉴스 정보 포함)
                        cur.execute("""
                            INSERT INTO theme_stocks (symbol, stock_name, theme_name, recent_news, updated_at) 
                            VALUES (%s, %s, %s, %s, NOW()) 
                            ON CONFLICT (symbol) DO UPDATE SET 
                                stock_name = EXCLUDED.stock_name,
                                theme_name = EXCLUDED.theme_name,
                                recent_news = EXCLUDED.recent_news,
                                updated_at = NOW();
                        """, (sym, final_name, n_theme, n_news))




                        if not data:
                            data = {'name': final_name, 'price': 0, 'profit': 0, 'value': 0, 'vol': 0, 'prev_vol': 0, 'is_rank_100': False}
                        # --- [네이버 데이터 활용 끝] ---

                        # --- [기존 보조 지표 계산 로직] ---
                        cur.execute("""
                            SELECT MAX(stck_hgpr), MAX(bb_upper), MAX(bb_lower)
                            FROM (
                                SELECT stck_hgpr, bb_upper, bb_lower 
                                FROM daily_stock_stats 
                                WHERE symbol = %s 
                                ORDER BY stck_bsop_date DESC LIMIT 5
                            ) as recent;
                        """, (sym,))
                        row = cur.fetchone()
                        
                        high_5d = float(row[0]) if row and row[0] else 0
                        bb_upper = float(row[1]) if row and row[1] else 0
                        bb_lower = float(row[2]) if row and row[2] else 0
                        
                        bb_status = 'NORMAL'
                        if data['price'] > 0 and bb_upper > 0:
                            if data['price'] >= bb_upper: bb_status = 'UPPER'
                            elif data['price'] <= bb_lower: bb_status = 'LOWER'

                        # 2. 점수 계산
                        score = 0
                        cap_times = {f'cap_time_{i}': None for i in range(1, 10)}
                        if data['is_rank_100']: score += 1; cap_times['cap_time_1'] = now
                        if data['profit'] >= 5.0: score += 1; cap_times['cap_time_2'] = now
                        if data['price'] > high_5d and high_5d > 0: score += 1; cap_times['cap_time_3'] = now
                        if data['prev_vol'] > 0 and (data['vol'] - data['prev_vol']) >= 5000: score += 1; cap_times['cap_time_4'] = now
                        
                        for k, v in self.target_cond_seqs.items():
                            if sym in hts_results.get(v, []): score += 1; cap_times[k] = now

                        # 3. detected_signals 업데이트
                        cur.execute("""
                            INSERT INTO detected_signals (
                                symbol, current_price, profit_rate, trade_value, volume, prev_volume, 
                                scores, high_5d, bb_status,
                                cap_time_1, cap_time_2, cap_time_3, cap_time_4, cap_time_6, cap_time_7, cap_time_8, cap_time_9, 
                                updated_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (symbol) DO UPDATE SET
                                current_price = EXCLUDED.current_price, 
                                profit_rate = EXCLUDED.profit_rate,
                                trade_value = EXCLUDED.trade_value, 
                                volume = EXCLUDED.volume, 
                                scores = EXCLUDED.scores,
                                high_5d = EXCLUDED.high_5d,
                                bb_status = EXCLUDED.bb_status,
                                cap_time_1 = COALESCE(detected_signals.cap_time_1, EXCLUDED.cap_time_1),
                                cap_time_2 = COALESCE(detected_signals.cap_time_2, EXCLUDED.cap_time_2),
                                cap_time_3 = COALESCE(detected_signals.cap_time_3, EXCLUDED.cap_time_3),
                                cap_time_4 = COALESCE(detected_signals.cap_time_4, EXCLUDED.cap_time_4),
                                cap_time_6 = COALESCE(detected_signals.cap_time_6, EXCLUDED.cap_time_6),
                                cap_time_7 = COALESCE(detected_signals.cap_time_7, EXCLUDED.cap_time_7),
                                cap_time_8 = COALESCE(detected_signals.cap_time_8, EXCLUDED.cap_time_8),
                                cap_time_9 = COALESCE(detected_signals.cap_time_9, EXCLUDED.cap_time_9),
                                updated_at = NOW()
                            RETURNING cap_time_1, cap_time_2, cap_time_3, cap_time_4, cap_time_5, cap_time_6, cap_time_7, cap_time_8, cap_time_9;
                        """, (
                            sym, data['price'], data['profit'], data['value'], data['vol'], data['prev_vol'], 
                            score, high_5d, bb_status,
                            cap_times['cap_time_1'], cap_times['cap_time_2'], cap_times['cap_time_3'], 
                            cap_times['cap_time_4'], cap_times['cap_time_6'], cap_times['cap_time_7'], 
                            cap_times['cap_time_8'], cap_times['cap_time_9']
                        ))
                        
                        db_times = cur.fetchone()

                        # 4. 1분 이력 기록 (signal_history)
                        cur.execute("""
                            INSERT INTO signal_history (symbol, current_price, profit_rate, trade_value, volume, scores, 
                                cap_time_1, cap_time_2, cap_time_3, cap_time_4, cap_time_5, cap_time_6, cap_time_7, cap_time_8, cap_time_9, recorded_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
                        """, (sym, data['price'], data['profit'], data['value'], data['vol'], score, *db_times))
                        
                        # 네이버 크롤링 사이 간격 (IP 차단 방지)
                        time.sleep(0.05)
                        
        except Exception as e:
            print(f"? 데이터 저장 오류: {e}")
        finally: 
            conn.close()
    
    def _save_theme_to_db(self, symbol, theme_name):
        conn = self._get_db_connection()
        if not conn: return
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO theme_stocks (symbol, theme_name, source, start_date, updated_at, end_date)
                        VALUES (%s, %s, 'NAVER', CURRENT_DATE, NOW(), '9999-12-31')
                        ON CONFLICT (symbol, theme_name) 
                        DO UPDATE SET updated_at = NOW(), end_date = '9999-12-31';
                    """, (symbol, theme_name))
        finally: conn.close()

    def update_theme_lifecycle(eslf, symbol, current_themes):
        """기획안: 소멸된 테마 처리 (DB에는 있는데 파싱 결과엔 없는 경우)"""
        conn = self._get_db_connection()
        if not conn: return
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT theme_name FROM theme_stocks WHERE symbol = %s AND end_date = '9999-12-31'", (symbol,))
                    db_themes = [row[0] for row in cur.fetchall()]
                    
                    for d_theme in db_themes:
                        if d_theme not in current_themes:
                            cur.execute("""
                                UPDATE theme_stocks SET end_date = CURRENT_DATE, updated_at = NOW()
                                WHERE symbol = %s AND theme_name = %s AND end_date = '9999-12-31'
                            """, (symbol, d_theme))
                            print(f"?? [Theme Out] {symbol}: '{d_theme}' 테마 제외됨")
        finally: conn.close()


    def fetch_daily_data(self):
        """장 마감 후 호출: 감시된 종목들의 일별 종가 및 볼린저 지표 저장"""
        conn = self._get_db_connection()
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM stock_master")
                symbols = [row[0] for row in cur.fetchall()]
            
            token = self.auth.get_access_token()
            headers = {
                "Content-Type": "application/json", "authorization": f"Bearer {token}",
                "appkey": self.api_key, "appsecret": self.api_secret,
                "tr_id": "FHPST01010400", "custtype": "P"
            }

            print(f"?? [Daily] {len(symbols)}개 종목 일일 데이터 수집 중...")
            for sym in symbols:
                params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": sym, "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"}
                res = requests.get(f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", headers=headers, params=params)
                if res.status_code == 200:
                    output = res.json().get('output2', [])
                    if not output: continue
                    
                    # 최신일 데이터
                    today_data = output[0]
                    # 볼린저밴드 계산 (최근 20일 종가 기준)
                    closes = [float(day['stck_clpr']) for day in output[:20]]
                    if len(closes) >= 20:
                        ma20 = np.mean(closes)
                        std20 = np.std(closes)
                        upper = ma20 + (std20 * 2)
                        lower = ma20 - (std20 * 2)
                    else:
                        upper, lower = 0, 0

                    with conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO daily_stock_stats (symbol, stck_bsop_date, stck_clpr, stck_hgpr, stck_lwpr, acml_vol, acml_tr_pbmn, bb_upper, bb_lower, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                                ON CONFLICT (symbol, stck_bsop_date) DO UPDATE SET
                                    stck_clpr = EXCLUDED.stck_clpr, bb_upper = EXCLUDED.bb_upper, bb_lower = EXCLUDED.bb_lower;
                            """, (sym, today_data['stck_bsop_date'], today_data['stck_clpr'], today_data['stck_hgpr'], today_data['stck_lwpr'], 
                                  today_data['acml_vol'], today_data['acml_tr_pbmn'], upper, lower))
                time.sleep(0.1)
        except Exception as e: print(f"? 일별 수집 오류: {e}")
        finally: conn.close()

    def run(self):
        print("[System] 통합 수집 엔진 가동 (1분 주기 + 네이버 크롤링)")
        self.fetch_hts_condition_list()
        
        count = 0
        while True:
            try:
                now_time = datetime.now()
                # 1. 장중 수집 (09:00 ~ 15:40)
                hts_results = {c_id: self.get_stocks_by_condition(c_id) for c_id in self.condition_map.values()}
                master_dict = self.fetch_kis_rank_data()
                
                if master_dict or any(hts_results.values()):
                    self.process_and_save(master_dict, hts_results)
                
                    # -------------------------------------------------------
                    # [기획안 3-① 추가] 네이버 테마 및 뉴스 동기화
                    # 방금 process_and_save를 통해 포착된 종목들을 포함하여 
                    # 최근 24시간 이내 포착 종목들의 테마/뉴스/생애주기를 실시간 동기화합니다.
                    # -------------------------------------------------------
                    print(f"📡 [{now_time.strftime('%H:%M:%S')}] 네이버 데이터(테마/뉴스) 실시간 매칭 중...")
                    self.sync_naver_data()

                # 2. 일별 데이터 업데이트 (장 마감 직후 16:00 또는 수집기 시작 시 1회 수행)
                if (now_time.hour == 16 and now_time.minute == 0) or count == 0:
                    self.fetch_daily_data()

                count += 1
                time.sleep(60)
                print(f"?? [{now_time.strftime('%H:%M:%S')}] 분석 사이클 완료")
            except Exception as e:
                print(f"?? 루프 오류: {e}"); time.sleep(10)

if __name__ == "__main__":
    collector = StockCollector()
    collector.run()