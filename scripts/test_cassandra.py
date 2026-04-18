from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Створюємо ОДИН тестовий рядок, який точно підходить під твою таблицю
data = [("2026-04", 1, "test_product_123")]
columns = ["period", "review_count", "product_id"]

test_df = spark.createDataFrame(data, columns)

try:
    test_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="product_stats_by_period", keyspace="amazon_reviews") \
        .mode("append") \
        .save()
    print("--- ТЕСТ УСПІШНИЙ: ЗАПИС ПІШОВ ---")
except Exception as e:
    print(f"--- ПОМИЛКА ТЕСТУ: {e} ---")

spark.stop()