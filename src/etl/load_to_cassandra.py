import os
from pathlib import Path  # Додай цей рядок
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_data():
    # Отримуємо абсолютний шлях до папки hadoop у твоєму проєкті
    base_path = Path(__file__).parent.parent.parent
    hadoop_home = str(base_path / "hadoop")
    
    # Вказуємо Spark, де шукати bin\winutils.exe
    os.environ['HADOOP_HOME'] = hadoop_home
    
    # 1. Ініціалізація Spark Session з конектором Cassandra
    spark = SparkSession.builder \
        .appName("ReviewsETL") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .getOrCreate()

    print(f"✅ HADOOP_HOME встановлено на: {hadoop_home}")
    print("📖 Reading interim Parquet data...")
    
    # Читаємо Parquet
    df = spark.read.parquet("data/interim/reviews_parquet")

    # Записуємо у 3 таблиці згідно з планом моделювання [cite: 46]
    tables = ["reviews_by_product", "reviews_by_product_rating", "reviews_by_customer"]

    for table_name in tables:
        print(f"🚀 Loading data into {table_name}...")
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table_name, keyspace="reviews_keyspace") \
            .mode("append") \
            .save()

    print("✅ Data successfully loaded into all Cassandra tables!")
    spark.stop()

if __name__ == "__main__":
    load_data()