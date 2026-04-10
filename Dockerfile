# Використовуємо Python-образ як базу
FROM python:3.11-slim

# Встановлюємо Java (вона потрібна Spark для роботи)
RUN apt-get update && apt-get install -y default-jdk procps && apt-get clean

WORKDIR /app

# Копіюємо requirements.txt і встановлюємо бібліотеки
# (Якщо файлу requirements.txt ще немає, створи пустий)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо весь інший код
COPY . .

ENV PYTHONPATH=/app