from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """
    Основні налаштування додатку (App Settings).
    
    Відповідальність: Зберігання параметрів для API, репозиторіїв та кешу.
    Використовується рівнем інфраструктури та сервісів для підключення до БД.
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
    
    class Config:
        # Автоматичне підвантаження значень зі змінних оточення або .env файлу
        env_file = ".env"

# Створення об'єкта налаштувань для використання в додатку
settings = Settings()

class ETLSettings(BaseSettings):
    """
    Спеціалізована конфігурація для ETL-процесів (Extract, Transform, Load).
    
    Призначення: Керування параметрами обробки великих даних через Apache Spark.
    Розподіл на AppSettings та ETLSettings дозволяє не тягнути зайві залежності 
    (наприклад, шляхи до файлів) у веб-API.
    """
    
    # ========== SPARK ==========
    # Параметри ініціалізації SparkSession
    spark_app_name: str = "Amazon Reviews ETL"
    spark_master: Optional[str] = None  # Режим роботи: local[*] для розробки, yarn/k8s для продакшену
    
    # ========== ШЛЯХИ ==========
    # Визначають "шлях" даних від сирого CSV до оптимізованого Parquet
    csv_input_path: str = "/opt/spark/data/raw/amazon_reviews.csv"
    parquet_output_path: str = "/opt/spark/data/processed/reviews.parquet"
    
    # Вхідна точка для фінального завантаження в БД
    parquet_input_path: str = "/opt/spark/data/processed/reviews.parquet"
    
    # ========== CASSANDRA ==========
    # Повторне визначення для ETL-контексту (може відрізнятися від API, наприклад, host "cassandra" у Docker)
    cassandra_host: str = "cassandra"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "amazon_reviews"
    
    # Цільові таблиці для завантаження даних (відповідають денормалізованим моделям)
    cassandra_table_customer: str = "reviews_by_customer"
    cassandra_table_product: str = "reviews_by_product"
    cassandra_table_analytics: str = "reviews_for_analytics"
    
    # ========== ETL ПАРАМЕТРИ ==========
    # Параметри стійкості до помилок (Fault Tolerance)
    max_retries: int = 3              # Кількість спроб перезапуску кроку при мережевих збоях
    retry_delay_seconds: int = 5      # Час очікування перед повторною спробою
    validate_data: bool = True        # Чи проводити структурну перевірку даних (Data Quality Check)
    
    # ========== SPARK КОНФІГИ ==========
    # Тонке налаштування рушія Spark. 
    # Вимкнення ANSI забезпечує гнучкість при обробці некоректних дат (замість Error буде NULL)
    spark_ansi_enabled: bool = False  
    
    class Config:
        # Пріоритет: змінні оточення (ENV) мають вищий пріоритет, ніж значення в .env
        env_file = ".env"
        env_file_encoding = "utf-8"


# Глобальний інстанс налаштувань для ETL-скриптів
settings = ETLSettings()