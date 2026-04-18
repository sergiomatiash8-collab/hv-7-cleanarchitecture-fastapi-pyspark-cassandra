Amazon Reviews Analytics API
PySpark + Cassandra + FastAPI + Redis

This project provides a high-performance REST API for querying and analyzing Amazon product reviews. It leverages Apache Spark for data processing, Apache Cassandra for scalable storage, FastAPI for the web interface, and Redis for efficient response caching.

Architecture Overview
The system follows a modern data engineering pipeline:

ETL Layer: PySpark processes raw Amazon review datasets, performs complex aggregations, and loads them into query-specific Cassandra tables.

Storage Layer: Cassandra is designed with a "Table-per-Query" approach to avoid ALLOW FILTERING and ensure O(1) or O(log n) lookup speeds.

API Layer: A FastAPI application provides endpoints for consumers.

Caching Layer: Redis stores API responses for 5 minutes (TTL 300s) to minimize database load.

Tech Stack
Language: Python 3.11+

Data Processing: Apache Spark (PySpark)

Database: Apache Cassandra 4.1

API Framework: FastAPI (Uvicorn)

Caching: Redis

Infrastructure: Docker & Docker Compose

Features & Endpoints
Product Analytics
GET /reviews/product/{product_id}: All reviews for a specific product.

GET /reviews/product/{product_id}/rating/{rating}: Filtered reviews by star rating.

GET /reviews/top-products?n=5&period=YYYY-MM: Top N most reviewed items for a specific month.

Customer Analytics
GET /reviews/customer/{customer_id}: History of reviews written by a specific user.

GET /reviews/top-customers?n=5&period=YYYY-MM: Most productive customers (verified purchases).

GET /reviews/top-haters?n=5&period=YYYY-MM: Customers with the most 1-2 star reviews.

GET /reviews/top-backers?n=5&period=YYYY-MM: Customers with the most 4-5 star reviews.

Setup & Installation
1. Environment Setup
Clone the repository and create a virtual environment:

Bash
git clone <your-repo-url>
cd hv-7-cassandra-pyspark-fastapi
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
2. Launch Infrastructure
Start all services using Docker Compose:

Bash
docker-compose up -d
3. Data Transformation (ETL)
Run the Spark job to process raw data and populate Cassandra:

Bash
docker exec -it spark-master spark-submit \
--packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
/app/src/scripts/etl_job.py
Performance & Design Choices
No ALLOW FILTERING: Every endpoint queries Cassandra using Partition Keys specifically designed for that query.

Redis Caching: All analytics endpoints implement a 5-minute cache.

Key Pattern: prefix:param1:param2

TTL: 300 seconds.

Data Consistency: Used dict_factory for Cassandra row mapping to ensure clean JSON serialization.

Verification
To verify the system status:

API Documentation: Check Swagger UI at http://localhost:8000/docs

Cassandra Data:
docker exec -it cassandra cqlsh -e "SELECT * FROM amazon_reviews.product_stats_by_period LIMIT 5;"

Redis Keys:
docker exec -it redis redis-cli KEYS *