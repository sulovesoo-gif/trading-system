FROM python:3.11-slim

WORKDIR /app

# 필요한 패키지 설치 (PostgreSQL client 등)
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# 의존성 파일 복사 및 설치
COPY services/collector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 서비스 소스 및 공통 코드(common) 복사
COPY services/collector/ .
COPY common/ /app/common/

# Python 경로 설정 (common 폴더 임포트 가능하게)
ENV PYTHONPATH=/app

CMD ["python", "main.py"]