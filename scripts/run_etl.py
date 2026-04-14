from etl.engine import get_spark_session
from etl.transformers import clean_reviews_data
from etl.loaders import load_to_cassandra

def run_main_pipeline():
    # 1. Запуск сесії
    spark = get_spark_session()
    
    try:
        # 2. Extract (Витягування)
        raw_df = spark.read.parquet("/opt/spark/data/processed/reviews.parquet")
        
        # 3. Transform (Очищення)
        clean_df = clean_reviews_data(raw_df)
        
        # 4. Load (Завантаження в різні таблиці)
        # Завантажуємо в таблицю по продуктах
        load_to_cassandra(
            clean_df.select("product_id", "star_rating", "review_id", "customer_id", "review_body", "review_date"),
            "reviews_by_product"
        )
        
        # Завантажуємо в таблицю по клієнтах
        load_to_cassandra(
            clean_df.select("customer_id", "review_date", "review_id", "product_id", "star_rating", "verified_purchase"),
            "reviews_by_customer"
        )
        
        print("✅ Усі етапи ETL завершено успішно!")
        
    except Exception as e:
        print(f"❌ Помилка під час виконання ETL: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_main_pipeline()