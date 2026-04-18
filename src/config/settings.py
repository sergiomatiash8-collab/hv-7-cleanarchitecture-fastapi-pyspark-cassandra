from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """
    Main application settings.
    
    Responsibility: Storing parameters for API, repositories, and cache.
    """
    
    cassandra_host: str = "127.0.0.1"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "amazon_reviews"
    
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    
    cache_ttl: int = 60
    
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class ETLSettings(BaseSettings):
    """
    Specialized configuration for ETL (Extract, Transform, Load) processes.
    """
    
    # ========== SPARK ==========
    spark_app_name: str = "Amazon Reviews ETL"
    spark_master: Optional[str] = None
    
    # ========== PATHS ==========
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
    
    # ========== ETL PARAMETERS ==========
    max_retries: int = 3
    retry_delay_seconds: int = 5
    validate_data: bool = True
    
    # ========== SPARK CONFIGS ==========
    spark_ansi_enabled: bool = False  
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


# Create objects
settings = Settings()
etl_settings = ETLSettings()