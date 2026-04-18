import os
import sys
from etl.engine import get_spark_session
from etl.transformers import (
    clean_reviews_data, get_product_stats, 
    get_customer_verified_stats, get_hater_stats, get_backer_stats
)
from etl.loaders import load_to_cassandra

def run_main_pipeline():
    # 1. Визначаємо абсолютний шлях до проєкту
    # Використовуємо abspath, щоб точно знати, де ми
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    data_path = os.path.join(project_root, "data", "processed", "reviews.parquet")
    
    print("-" * 50)
    print(f"DEBUG: Current working directory: {os.getcwd()}")
    print(f"DEBUG: Project root identified as: {project_root}")
    print(f"DEBUG: Looking for Parquet at: {data_path}")
    
    # ПЕРЕВІРКА: чи існує файл на диску
    if not os.path.exists(data_path):
        print(f"❌ КРИТИЧНА ПОМИЛКА: Файл НЕ знайдено за шляхом {data_path}")
        print("Перевір, чи папка 'data/processed' знаходиться в корені проєкту.")
        return

    print("✅ Файл знайдено! Запускаємо Spark...")
    print("-" * 50)

    spark = get_spark_session()
    
    try:
        # 2. Extract
        # Додаємо префікс file:// для Windows, щоб Spark точно зрозумів локальний шлях
        absolute_uri = "file:///" + data_path.replace("\\", "/")
        raw_df = spark.read.parquet(absolute_uri)
        
        # 3. Transform
        clean_df = clean_reviews_data(raw_df).cache()
        
        # 4. Load: Базові таблиці
        print("--- Loading basic tables ---")
        load_to_cassandra(
            clean_df.select("product_id", "star_rating", "review_id", "customer_id", "review_body", "review_date"),
            "reviews_by_product"
        )
        load_to_cassandra(
            clean_df.select("customer_id", "review_date", "review_id", "product_id", "star_rating", "verified_purchase"),
            "reviews_by_customer"
        )
        
        # 5. Load: Аналітика
        print("--- Loading analytics ---")
        load_to_cassandra(get_product_stats(clean_df), "product_stats_by_period")
        load_to_cassandra(get_customer_verified_stats(clean_df), "customer_verified_stats_by_period")
        load_to_cassandra(get_hater_stats(clean_df), "hater_stats_by_period")
        load_to_cassandra(get_backer_stats(clean_df), "backer_stats_by_period")
        
        print("✅ Успіх! Всі дані в Cassandra.")
        clean_df.unpersist()
        
    except Exception as e:
        print(f"❌ Помилка в Pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_main_pipeline()