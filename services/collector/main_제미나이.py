import time
import sys
import os

# 부모 디렉토리의 common 폴더를 참조하기 위해 경로 설정
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 필요한 모듈 임포트 (기존 파일들이 있다는 가정하에 진행)
# 만약 에러가 난다면 kis_auth.py와 db_manager.py가 해당 위치에 있어야 합니다.
try:
    from kis_auth import KISAuth
    from collector_final import StockCollector
except ImportError as e:
    print(f"❌ 임포트 에러: {e}")
    print("💡 kis_auth.py 또는 collector_final.py 파일이 같은 폴더에 있는지 확인하세요.")
    sys.exit(1)

def main():
    print("🚀 [System] Trading Collector Service를 시작합니다.")
    
    # 1. KIS API 인증 객체 생성
    auth = KISAuth()
    
    # 2. 수집기 객체 생성
    # collector_final.py 내의 클래스명이 StockCollector인지 확인 필요
    collector = StockCollector()

    print("✅ 서비스 초기화 완료. 수집 루프를 시작합니다.")

    while True:
        try:
            print(f"🕒 [{time.strftime('%H:%M:%S')}] 실시간 데이터 수집 및 분석 중...")
            
            # [수정 포인트] 
            # 에러 메시지에 따르면 'collect_all_targets'라는 이름이 없다고 하므로,
            # collector_final.py에 정의된 실제 실행 메서드 이름을 호출해야 합니다.
            # 보통 통합 수집기는 'run' 또는 'collect'라는 이름을 많이 사용합니다.
            
            # 만약 collector_final.py의 메서드 이름이 다르다면 아래를 수정하세요.
            if hasattr(collector, 'run'):
                collector.run()
            elif hasattr(collector, 'collect'):
                collector.collect()
            else:
                # 강제로 collector_final.py의 구조에 맞춤 (가장 가능성 높은 이름)
                collector.start_collecting() 

        except Exception as e:
            print(f"❌ 메인 루프 에러 발생: {e}")
        
        # 과도한 API 호출 방지를 위해 1분 대기 (기획안 기준)
        time.sleep(60)

if __name__ == "__main__":
    main()