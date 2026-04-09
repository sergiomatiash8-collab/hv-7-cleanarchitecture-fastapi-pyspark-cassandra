import os
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Windows-Fix") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

try:
    # 1. Читаємо CSV
    df = spark.read.csv("data/raw/amazon_reviews.csv", header=True, inferSchema=True)
    
    # 2. Конвертуємо в Pandas (це обходить всі проблеми з winutils)
    # Для домашки об'єм даних дозволяє це зробити без проблем
    p_df = df.toPandas()
    
    # 3. Створюємо папку вручну
    os.makedirs("data/silver", exist_ok=True)
    
    # 4. Пишемо файл
    p_df.to_parquet("data/silver/reviews.parquet", engine='pyarrow', index=False)
    
    print("\n" + "!"*20)
    print("НАРЕШТІ! Файл записано в data/silver/reviews.parquet")
    print("!"*20)

except Exception as e:
    print(f"\nзнову помилка: {e}")
finally:
    spark.stop()