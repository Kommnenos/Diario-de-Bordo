FROM openjdk:11-jdk-slim

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip

RUN pip3 install requirements.txt

WORKDIR /app

COPY . .

CMD ["python3", "main.py"]

