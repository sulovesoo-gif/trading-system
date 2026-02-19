import requests
from bs4 import BeautifulSoup
from services.common.db_manager import get_db_connection

def fetch_and_save_themes():
    print("🌐 네이버 금융에서 테마 정보를 읽어오는 중...")
    url = "https://finance.naver.com/sise/theme.naver"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    
    try:
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # [핵심 변경] 모든 <a> 태그 중 테마 상세 페이지 링크만 필터링
        all_links = soup.find_all('a')
        theme_links = []
        for a in all_links:
            href = a.get('href', '')
            # 테마 상세 페이지 주소 규칙: /sise/theme.naver?field=name&ordering=asc&item_code=...
            if 'theme.naver?field=' in href and 'item_code=' in href:
                if a.text.strip(): # 텍스트가 있는 것만
                    theme_links.append(a)

        if not theme_links:
            print("❌ 테마 링크를 추출하지 못했습니다. 구조를 재점검합니다.")
            return

        print(f"🔎 총 {len(theme_links)}개의 테마를 찾았습니다. 수집을 시작합니다.")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 너무 많으면 오래 걸리니 상위 20개만 먼저 수집해봅시다.
                for a in theme_links[:20]:
                    theme_name = a.text.strip()
                    theme_link = "https://finance.naver.com" + a['href']
                    
                    cur.execute("""
                        INSERT INTO themes (theme_name) VALUES (%s) 
                        ON CONFLICT (theme_name) DO UPDATE SET updated_at = NOW() 
                        RETURNING theme_id
                    """, (theme_name,))
                    theme_id = cur.fetchone()[0]
                    
                    # 상세 페이지로 들어가서 종목 긁기
                    res_detail = requests.get(theme_link, headers=headers)
                    soup_detail = BeautifulSoup(res_detail.text, 'html.parser')
                    
                    # 상세페이지 종목 링크 규칙: /item/main.naver?code=...
                    stock_links = soup_detail.find_all('a')
                    count = 0
                    for s in stock_links:
                        s_href = s.get('href', '')
                        if '/item/main.naver?code=' in s_href:
                            stock_name = s.text.strip()
                            symbol = s_href.split('=')[-1]
                            
                            if stock_name and len(symbol) == 6: # 종목명 있고 코드가 6자리인 것만
                                cur.execute("""
                                    INSERT INTO theme_stocks (theme_id, symbol, stock_name) 
                                    VALUES (%s, %s, %s) 
                                    ON CONFLICT (theme_id, symbol) DO NOTHING
                                """, (theme_id, symbol, stock_name))
                                count += 1
                    
                    print(f"  ✅ '{theme_name}' 완료 ({count} 종목 저장)")
                
                conn.commit()
        print("\n🚀 모든 테마 데이터가 DB에 저장되었습니다!")
        
    except Exception as e:
        print(f"❌ 에러 발생: {e}")

if __name__ == "__main__":
    fetch_and_save_themes()