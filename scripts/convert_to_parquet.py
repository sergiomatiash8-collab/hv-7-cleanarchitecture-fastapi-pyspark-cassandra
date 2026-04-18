from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Convert").getOrCreate()


csv_path = "/opt/spark/data/raw/amazon_reviews.csv"

if os.path.exists(csv_path):
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
   
    df.write.mode("overwrite").parquet("/opt/spark/data/processed/reviews.parquet")
    print("--- УСПІШНО: CSV ПЕРЕТИСНУТО В PARQUET ---")
else:
    print(f"--- ПОМИЛКА: ФАЙЛ {csv_path} НЕ ЗНАЙДЕНО ---")