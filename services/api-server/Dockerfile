# Node.js 20 버전 사용 (사용자 설정 유지)
FROM node:20-alpine

WORKDIR /app

# 1. 루트 기준에서 api-server 폴더 내의 package 파일을 복사
COPY services/api-server/package*.json ./

# 2. 깨끗한 설치를 위해 npm ci 사용 (package-lock.json 기반)
RUN npm ci

# 3. api-server 내의 모든 소스 코드를 컨테이너의 /app으로 복사
COPY services/api-server/ .

# [옵션] 나중에 common 폴더를 공유해야 한다면 아래 주석을 해제하세요.
# COPY common/ /app/common/

EXPOSE 4000

# 4. 실행 명령 (package.json의 scripts에 start가 정의되어 있어야 함)
CMD ["npm", "run", "start"]