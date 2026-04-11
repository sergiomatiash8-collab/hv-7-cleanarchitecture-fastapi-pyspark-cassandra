import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_data():
    # Налаштування шляхів
    base_path = Path(__file__).parent.parent.parent
    os.environ['HADOOP_HOME'] = str(base_path / "hadoop")
    
    parquet_path = str(base_path / "data" / "interim" / "reviews_parquet" / "reviews.parquet")
    
    # Створення сесії без зайвих відступів та коментарів всередині
    spark = SparkSession.builder \
        .appName("ReviewsETL") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0") \
        .getOrCreate()

    print("🚀 Spark ініціалізовано. Починаємо завантаження...")

    try:
        if not os.path.exists(parquet_path):
            print(f"❌ Файл не знайдено: {parquet_path}")
            return

        df = spark.read.parquet(parquet_path)
        
        # Unified naming: created_at
        df_filtered = df.select(
            "review_id", 
            "product_id", 
            "customer_id",
            "star_rating", 
            "review_body",
            F.col("review_date").alias("created_at")
        )

        tables = ["reviews", "reviews_by_product", "reviews_by_customer"]

        for table_name in tables:
            print(f"⏳ Запис у {table_name}...")
            df_filtered.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=table_name, keyspace="reviews_keyspace") \
                .mode("append") \
                .save()
            print(f"✅ {table_name} — готово.")

    except Exception as e:
        print(f"❌ Помилка: {e}")
    finally:
        spark.stop()
        print("🛑 Spark зупинено.")

if __name__ == "__main__":
    load_data()