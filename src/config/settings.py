from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Cassandra
    cassandra_host: str = "127.0.0.1"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "amazon_reviews"
    
    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    
    # Cache
    cache_ttl: int = 60
    
    class Config:
        env_file = ".env"

settings = Settings()

class ETLSettings(BaseSettings):
    """
    Конфігурація для всіх ETL процесів
    Читає з .env файлу або використовує defaults
    """
    
    # ========== SPARK ==========
    spark_app_name: str = "Amazon Reviews ETL"
    spark_master: Optional[str] = None  # local[*] або None для auto
    
    # ========== ШЛЯХИ ==========
    # CSV → Parquet
    csv_input_path: str = "/opt/spark/data/raw/amazon_reviews.csv"
    parquet_output_path: str = "/opt/spark/data/processed/reviews.parquet"
    
    # Parquet → Cassandra
    parquet_input_path: str = "/opt/spark/data/processed/reviews.parquet"
    
    # ========== CASSANDRA ==========
    cassandra_host: str = "cassandra"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "amazon_reviews"
    
    # Таблиці для load_to_cassandra.py
    cassandra_table_customer: str = "reviews_by_customer"
    cassandra_table_product: str = "reviews_by_product"
    cassandra_table_analytics: str = "reviews_for_analytics"
    
    # ========== ETL ПАРАМЕТРИ ==========
    max_retries: int = 3              # Скільки разів повторювати при помилці
    retry_delay_seconds: int = 5      # Затримка між спробами
    validate_data: bool = True        # Чи валідувати дані перед записом
    
    # ========== SPARK КОНФІГИ ==========
    spark_ansi_enabled: bool = False  # Вимкнено, щоб не падало на невалідних датах
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Глобальний інстанс налаштувань
settings = ETLSettings()