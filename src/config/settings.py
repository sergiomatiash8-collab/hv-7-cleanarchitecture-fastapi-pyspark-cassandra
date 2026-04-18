from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """
    Основні налаштування додатку (App Settings).
    
    Відповідальність: Зберігання параметрів для API, репозиторіїв та кешу.
    """
    # Cassandra: параметри підключення до основного сховища
    cassandra_host: str = "127.0.0.1"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "amazon_reviews"
    
    # Redis: налаштування для шару кешування
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    
    # Cache: налаштування життєвого циклу даних у кеші (Time To Live)
    cache_ttl: int = 60
    
    # Додаємо extra="ignore", щоб Pydantic не падав, якщо в .env є зайві змінні (наприклад, для Spark)
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class ETLSettings(BaseSettings):
    """
    Спеціалізована конфігурація для ETL-процесів (Extract, Transform, Load).
    """
    
    # ========== SPARK ==========
    spark_app_name: str = "Amazon Reviews ETL"
    spark_master: Optional[str] = None
    
    # ========== ШЛЯХИ ==========
    csv_input_path: str = "/opt/spark/data/raw/amazon_reviews.csv"
    parquet_output_path: str = "/opt/spark/data/processed/reviews.parquet"
    parquet_input_path: str = "/opt/spark/data/processed/reviews.parquet"
    
    # ========== CASSANDRA ==========
    cassandra_host: str = "cassandra"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "amazon_reviews"
    
    cassandra_table_customer: str = "reviews_by_customer"
    cassandra_table_product: str = "reviews_by_product"
    cassandra_table_analytics: str = "reviews_for_analytics"
    
    # ========== ETL ПАРАМЕТРИ ==========
    max_retries: int = 3
    retry_delay_seconds: int = 5
    validate_data: bool = True
    
    # ========== SPARK КОНФІГИ ==========
    spark_ansi_enabled: bool = False  
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


# Створюємо об'єкти
settings = Settings()
etl_settings = ETLSettings()