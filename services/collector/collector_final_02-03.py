import time
import re
import requests
import os
import psycopg2
import pytz
import sys
import io
from datetime import datetime
from bs4 import BeautifulSoup
from kis_auth import KISAuth

class StockCollector:
    def __init__(self):
        self.auth = KISAuth()
        self.api_key = self.auth.api_key
        self.api_secret = self.auth.api_secret
        self.base_url = self.auth.base_url
        # 어제 쓰시던 환경변수 방식 그대로 유지
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'trading_db'),
            'user': os.getenv('POSTGRES_USER', 'admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'stock123'),
            'port': 5432
        }
        # 오늘 추가된 볼린저 밴드 조건식 번호
        self.condition_ids = {
            'bb_lower_touch': '0',   # 하단 터치
            'bb_upper_touch': '1',   # 상단 터치
            'bb_lower_break': '2',   # 하한선 + 거래량
            'bb_upper_break': '3'    # 상한선 + 거래량
        }

    def _get_db_connection(self):
        try:
            conn = psycopg2.connect(**self.db_params)
            # [결정적 한 줄] 데이터가 지나가는 통로를 UTF-8로 강제 고정합니다.
            conn.set_client_encoding('UTF8') 
            return conn
        except Exception as e:
            print(f"❌ DB 연결 에러: {e}")
            raise

    def _get_common_headers(self, tr_id):
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.auth.get_access_token()}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }

    # --- 1. 네이버 크롤링 ---
    def collect_naver_themes(self, target_symbols=None):
        # 시간 설정
        kst = pytz.timezone('Asia/Seoul')
        # 현재시간
        now_kst = datetime.now(kst).strftime('%H:%M:%S')
        
        # 분석된 종목이 있으면 그 종목들을 대상으로, 없으면 기본 샘플로 실행(샘플 5개인데 내가 원하는 주식으로 고정하자)
        symbols = target_symbols if target_symbols else ['005930', '000660', '066570', '035720', '035420']
        
        print(f"🔍 [{now_kst}] 네이버 테마 정밀 매칭 시작 (대상: {len(symbols)}종목)...")
        
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"}

        for symbol in symbols:
            try:
                # 네이버 금융 종목 메인 페이지
                url = f"https://finance.naver.com/item/main.naver?code={symbol}"
                res = requests.get(url, headers=headers, timeout=5)
                html_content = res.content
                
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # 종목명 추출
                name_tag = soup.select_one('div.wrap_company > h2 > a')
                stock_name = name_tag.get_text(strip=True) if name_tag else "알수없음"

                # [핵심] 해당 종목이 속한 테마 리스트 추출
                themes = re.findall(r'no=(\d+)[^>]*>([^<]+)</a>', html_content.decode('euc-kr', 'ignore'))
                
                valid_count = 0
                for t_no, t_name in set(themes):
                    t_name = t_name.strip()
                    # 무의미한 텍스트 필터링
                    if len(t_name) > 1 and t_name not in ['더보기', '테마별', 'PER', '종목토론실']:
                        # 1. 마스터 정보 및 시그널 테이블 동기화 (한글명 업데이트)
                        self._sync_to_signals(symbol, stock_name)
                        # 2. 테마-종목 매핑 저장
                        self._save_theme_to_db(symbol, t_name, "NAVER_ITEM")
                        valid_count += 1

                if valid_count > 0:
                    print(f"✅ {stock_name}({symbol}): {valid_count}개 테마 연결")
                
                # 네이버 차단 방지를 위한 미세 대기
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ {symbol} 테마 수집 중 에러: {e}")

    def _scrape_theme_details(self, theme_name, url, headers):
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.encoding = 'euc-kr'
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # 상세 페이지 종목 추출 (td.name 클래스 내부의 a 태그)
            stock_links = soup.select('td.name a')
            
            stocks_found = 0
            for s in stock_links:
                code = s['href'].split('=')[-1]
                name = s.get_text(strip=True)
                if code and name:
                    self._sync_to_signals(code, name)
                    self._save_theme_to_db(code, theme_name, "NAVER_THEME")
                    stocks_found += 1
            
            print(f"   -> {theme_name}: {stocks_found}개 종목 저장 완료")

        except Exception as e:
            print(f"❌ 상세 에러 ({theme_name}): {e}")

    def _sync_to_signals(self, symbol, name):
        try:
            kst = pytz.timezone('Asia/Seoul')
            now_kst = datetime.now(kst) # 이미 아주 잘 만드셨습니다!
            
            clean_name = name.strip() if name else "알수없음"
            
            with self._get_db_connection() as conn:
                with conn.cursor() as cur:
                    # 1. Master 저장 (now_kst 사용)
                    cur.execute("""
                        INSERT INTO stock_master (symbol, stock_name, updated_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (symbol) 
                        DO UPDATE SET stock_name = EXCLUDED.stock_name, updated_at = EXCLUDED.updated_at;
                    """, (symbol, clean_name, now_kst))
                    
                    # 2. Signals 저장 (NOW()를 지우고 %s 자리에 now_kst를 넣으세요)
                    cur.execute("""
                        INSERT INTO detected_signals (symbol, current_price, profit_rate, updated_at)
                        VALUES (%s, 0, 0, %s) 
                        ON CONFLICT (symbol) 
                        DO UPDATE SET updated_at = EXCLUDED.updated_at;
                    """, (symbol, now_kst)) # <--- %s 자리에 now_kst가 들어가야 합니다!
                conn.commit()
        except Exception as e:
            print(f"❌ 동기화 에러: {e}")
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

    # --- [신규] 주도주 + 볼린저 밴드 통합 분석 엔진 ---
    def detect_market_leaders(self):
        now = datetime.now()
        print(f"🚀 [{now.strftime('%H:%M:%S')}] 주도주 분석 및 볼린저 체크...")
        
        # [핵심] 헤더를 여기서 딱 한 번만 만듭니다. (반복 호출 금지)
        common_headers = self._get_common_headers("FHPST01710000")
        
        master_dict = {}

        # 1. KIS 상위 100위 수집 (거래대금)
        url_rank = f"{self.base_url}/uapi/domestic-stock/v1/quotations/volume-rank"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": "0", "FID_TR_GT_LS_CLS_CODE": "0",
            "FID_VOL_CNTN": "0", "FID_INPUT_DATE_1": ""
        }
        
        try:
            res = requests.get(url_rank, headers=common_headers, params=params).json()
            output = res.get('output', [])
            for item in output:
                sym = item['mksc_shrn_iscd']
                master_dict[sym] = self._init_data(item)
                if int(item.get('data_rank', 999)) <= 100:
                    master_dict[sym]['cap_time_1'] = now
        except Exception as e: 
            print(f"❌ KIS API 호출 에러: {e}")

        # 2. HTS 조건식(볼린저 밴드) 합치기
        # 볼린저 전용 TR ID로 헤더만 살짝 바꿔서 재사용
        c_headers = common_headers.copy()
        c_headers["tr_id"] = "HHKST03060000"
        
        mapping = {
            'bb_lower_touch': 'cap_time_6', 'bb_upper_break': 'cap_time_7', 
            'bb_lower_break': 'cap_time_8', 'bb_upper_touch': 'cap_time_9'
        }

        for key, seq in self.condition_ids.items():
            try:
                c_url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/psearch-result"
                c_params = {"user_id": os.getenv("KIS_USER_ID"), "seq": seq}
                c_res = requests.get(c_url, headers=c_headers, params=c_params).json()
                
                for s in c_res.get('output', []):
                    sym = s['code']
                    if sym not in master_dict: 
                        master_dict[sym] = self._init_data(s)
                    master_dict[sym][mapping[key]] = now
            except Exception: 
                continue

        if master_dict:
            self._save_integrated_signals(master_dict)

    def _init_data(self, raw):
        return {
            'name': raw.get('hts_kor_isnm', '알수없음'),
            'price': float(raw.get('stck_prpr', 0)),
            'profit': float(raw.get('prdy_ctrt', 0)),
            'value': int(raw.get('acml_tr_pbmn', 0)),
            'vol': int(raw.get('acml_vol', 0)),
            'prev_vol': int(raw.get('prdy_vol', 0))
        }

    def _save_integrated_signals(self, master_dict):
        now = datetime.now()
        try:
            with self._get_db_connection() as conn:
                with conn.cursor() as cur:
                    for sym, d in master_dict.items():
                        # 점수 계산: 100위권 진입(1점) + 볼린저 조건(1점)
                        score = 1 if d.get('cap_time_1') else 0
                        if any([d.get(f'cap_time_{i}') for i in range(6, 10)]): score += 1

                        # [핵심] detected_signals 업데이트 (실시간 현황판)
                        cur.execute("""
                            INSERT INTO detected_signals (
                                symbol, current_price, profit_rate, trade_value, volume, prev_volume, 
                                scores, cap_time_1, cap_time_6, cap_time_7, cap_time_8, cap_time_9, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol) DO UPDATE SET
                                current_price = EXCLUDED.current_price,
                                profit_rate = EXCLUDED.profit_rate,
                                trade_value = EXCLUDED.trade_value,
                                volume = EXCLUDED.volume,
                                scores = EXCLUDED.scores,
                                updated_at = EXCLUDED.updated_at,
                                # 최초 포착 시간은 유지 (COALESCE)
                                cap_time_1 = COALESCE(detected_signals.cap_time_1, EXCLUDED.cap_time_1),
                                cap_time_6 = COALESCE(detected_signals.cap_time_6, EXCLUDED.cap_time_6),
                                cap_time_7 = COALESCE(detected_signals.cap_time_7, EXCLUDED.cap_time_7),
                                cap_time_8 = COALESCE(detected_signals.cap_time_8, EXCLUDED.cap_time_8),
                                cap_time_9 = COALESCE(detected_signals.cap_time_9, EXCLUDED.cap_time_9);
                        """, (sym, d['price'], d['profit'], d['value'], d['vol'], d['prev_vol'], score,
                              d.get('cap_time_1'), d.get('cap_time_6'), d.get('cap_time_7'), 
                              d.get('cap_time_8'), d.get('cap_time_9'), now))
                    
                    # [어제 약속] 히스토리 저장을 위한 스냅샷 호출
                    # 1분 단위로 현재의 detected_signals 전체를 history 테이블로 복사합니다.
                    cur.execute("""
                        INSERT INTO signal_history (symbol, price, profit, volume, value, scores, recorded_at)
                        SELECT symbol, current_price, profit_rate, volume, trade_value, scores, NOW()
                        FROM detected_signals
                        WHERE updated_at >= NOW() - INTERVAL '1 minute';
                    """)

                conn.commit()
        except Exception as e:
            print(f"❌ 데이터베이스 통합 저장/히스토리 생성 실패: {e}")

    def run(self):
        print("💡 통합 수집 시스템 가동!")
        while True:
            try:
                # 1. KIS 주도주 & 볼린저 분석 (master_dict 반환하도록 수정 필요)
                # 이 함수 실행 결과로 추출된 종목 코드 리스트를 가져옵니다.
                self.detect_market_leaders()
                
                # 2. 방금 분석된 따끈따끈한 주도주들의 테마를 네이버에서 갱신
                # (DB에 저장된 오늘 자 주도주 리스트를 가져와서 돌리는 게 가장 깔끔합니다)
                with self._get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT symbol FROM detected_signals WHERE updated_at >= CURRENT_DATE")
                        today_symbols = [row[0] for row in cur.fetchall()]
                
                if today_symbols:
                    self.collect_naver_themes(today_symbols)
                
                print(f"💤 [{datetime.now().strftime('%H:%M:%S')}] 1분 대기 중...")
                time.sleep(60)
                
            except Exception as e:
                print(f"❗ 루프 에러: {e}")
                time.sleep(10)

if __name__ == "__main__":
    collector = StockCollector()
    print("🛠️ [테스트] 네이버 테마 수집을 즉시 시작합니다...")
    collector.collect_naver_themes()
    print("✅ 수집 완료! 이제 대시보드를 확인하세요.")