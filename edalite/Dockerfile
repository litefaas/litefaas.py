# Python 3.10 베이스 이미지 사용
FROM python:3.10-slim

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 패키지 설치를 위한 requirements.txt 생성 및 복사
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY main.py .

# 컨테이너 실행 시 실행할 명령어
CMD ["python", "main.py"]
