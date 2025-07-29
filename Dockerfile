FROM openjdk:17-slim

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip

COPY requirements.txt .

RUN pip3 install -r requirements.txt

WORKDIR /app

COPY . .

CMD ["python3", "main.py"]

