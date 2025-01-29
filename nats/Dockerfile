# 공식 NATS 이미지를 기반으로 사용
FROM nats:latest

# 메타데이터 추가
LABEL maintainer="Your Name <your.email@example.com>"
LABEL description="NATS 메시징 서버"

# 기본 설정 파일이 필요한 경우 복사
# COPY config/nats-server.conf /etc/nats/nats-server.conf

# NATS 서버의 기본 포트들
EXPOSE 4222 8222 6222

# 기본 명령어는 nats:latest 이미지에 이미 설정되어 있으므로 
# 별도의 CMD는 필요하지 않습니다.
