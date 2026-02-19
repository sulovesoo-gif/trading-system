import os

# 현재 파일(config.py)의 위치를 기준으로 프로젝트 루트 경로 계산
# services/common/config.py 기준 두 단계를 올라가면 루트입니다.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_env_path():
    """ .env 파일의 절대 경로 반환 """
    return os.path.join(BASE_DIR, ".env")