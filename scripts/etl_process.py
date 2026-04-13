import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.ERROR)

def main():
    # Додаємо конфігурацію для Cassandra
    spark = SparkSession.builder \
        .appName("LoadToCassandra") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()
    
    input_path = "/opt/spark/data/processed/reviews.parquet"
    
    try:
        # Читаємо наш готовий Parquet
        df = spark.read.parquet(input_path)
        
        print(f"Завантажуємо {df.count()} рядків у Cassandra...")

        # ЗАПИС У CASSANDRA
        # Переконайся, що keyspace 'amazon_reviews' та таблиця 'reviews' вже створені!
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="reviews", keyspace="amazon_reviews") \
            .mode("append") \
            .save()
            
        print("="*50)
        print("ДАНІ УСПІШНО ЗАЛИТО!")
        print("="*50)

    except Exception as e:
        print(f"ПОМИЛКА ПРИ ЗАПИСІ: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()