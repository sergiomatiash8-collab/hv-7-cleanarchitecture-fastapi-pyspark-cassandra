FROM python:3.11-slim

# Встановлюємо Java (потрібна для Spark)
RUN apt-get update && apt-get install -y default-jdk procps && apt-get clean

WORKDIR /app

# Налаштовуємо змінні для Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app