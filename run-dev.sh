#!/bin/bash

echo "🚀 Starting Development Environment..."

# 1. Запускаємо інфраструктуру (Cassandra, Redis, Spark)
docker-compose -f environments/dev/docker-compose.dev.yml up -d

echo "⏳ Waiting for databases to be ready..."
sleep 10 # Даємо час Cassandra стартувати

# 2. Запускаємо ETL: Конвертація в Parquet (локально або в контейнері)
echo "📦 Step 1: Converting CSV to Parquet..."
python src/etl/convert_to_parquet.py

# 3. Запускаємо ETL: Завантаження в Cassandra через Spark
echo "📥 Step 2: Loading data to Cassandra via Spark..."
docker exec -it fast_api_app python src/etl/load_to_cassandra.py

# 4. Запускаємо Аналітику
echo "📊 Step 3: Running Analytics (Haters, Backers, Top Products)..."
docker exec -it fast_api_app python src/analytics/run_analytics.py

echo "🌐 API is ready at http://localhost:8000"
echo "✅ All steps completed!"