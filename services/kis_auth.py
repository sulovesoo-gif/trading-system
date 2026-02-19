import os
import requests
import json
import yaml
import datetime

class KISAuth:
    def __init__(self):
        # 1. 설정 파일 로드
        with open("kis_devlp.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.load(f, Loader=yaml.FullLoader)
            
        self.api_key = cfg['my_app']
        self.api_secret = cfg['my_sec']
        self.user_id = cfg.get('my_htsid')
        # 공식 가이드 3.5절에 명시된 User-Agent 추가
        self.user_agent = cfg.get('my_agent', "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        self.base_url = "https://openapi.koreainvestment.com:9443"
        
        # 2. 토큰 저장 경로 (3.6절 참조)
        self.token_path = os.path.join(os.path.expanduser("~"), "KIS", "token.json")
        os.makedirs(os.path.dirname(self.token_path), exist_ok=True)

    def get_access_token(self):
        # 1. 기존 토큰 존재 확인
        if os.path.exists(self.token_path):
            with open(self.token_path, "r") as f:
                token_data = json.load(f)
                
                # 유효 기간 확인 (공식 명세의 만료 시간 필드 활용)
                # 만료 시간 형식: "2024-05-20 15:30:00"
                expired_str = token_data.get('access_token_token_expired')
                if expired_str:
                    expired_at = datetime.datetime.strptime(expired_str, '%Y-%m-%d %H:%M:%S')
                    # 현재 시간보다 만료 시간이 10분 이상 넉넉히 남았을 때만 사용
                    if expired_at > datetime.datetime.now() + datetime.timedelta(minutes=10):
                        return token_data.get('access_token')
                    else:
                        print("?? [KIS] 토큰 만료 임박 또는 만료됨. 재발급을 시도합니다.")

        # 2. 토큰 신규 발급 (파일이 없거나 만료되었을 때 실행)
        url = f"{self.base_url}/oauth2/tokenP"
        headers = {"Content-Type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.api_key,
            "appsecret": self.api_secret
        }

        res = requests.post(url, headers=headers, data=json.dumps(body))
        
        if res.status_code == 200:
            token_data = res.json()
            # 팁: 발급 성공 시 'access_token_token_expired' 필드가 포함되어 내려옵니다.
            with open(self.token_path, "w") as f:
                json.dump(token_data, f)
            print("? [KIS] 신규 토큰 발급 및 저장 성공!")
            return token_data.get('access_token')
        else:
            print(f"? 토큰 발급 실패: {res.text}")
            return None