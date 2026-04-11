from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_data():
    print("🚀 Starting Spark session...")
    
    spark = SparkSession.builder \
        .appName("CSVtoCassandra") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "172.25.0.2") \
        .getOrCreate()

    print("📦 Reading Parquet data...")
    df = spark.read.parquet("data/interim/reviews_parquet")

    # КОНВЕРТАЦІЯ ТИПУ: Cassandra Connector часто конфліктує з LocalDateTime
    # Перетворюємо створену дату в Timestamp, який Spark-Connector вміє мапити
    print("⚙️ Converting timestamp types...")
    df = df.withColumn("created_at", F.col("created_at").cast("timestamp"))

    print("📥 Loading data into Cassandra...")
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(
            table="reviews", 
            keyspace="review_keyspace"
        ) \
        .mode("append") \
        .save()

    print("✅ Data loaded successfully!")
    spark.stop()

if __name__ == "__main__":
    load_data()