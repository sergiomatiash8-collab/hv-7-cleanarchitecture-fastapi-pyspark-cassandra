from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, trim

# Ініціалізація Spark з налаштуваннями для Cassandra та вимкненим ANSI режимом
spark = SparkSession.builder \
    .appName("AmazonReviewsETL") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

try:
    # 1. Читання даних з Parquet
    df = spark.read.parquet("/opt/spark/data/processed/reviews.parquet")

    # 2. Очищення даних: 
    # - Прибираємо пробіли навколо дати
    # - Конвертуємо типи, щоб вони відповідали схемі Cassandra
    # - Фільтруємо записи, де дата не розпарсилася (наприклад, текстовий сміття)
    clean_df = df.withColumn("review_date", to_date(trim(col("review_date")), "yyyy-MM-dd")) \
                 .withColumn("star_rating", col("star_rating").cast("int")) \
                 .withColumn("verified_purchase", col("verified_purchase").cast("string")) \
                 .filter(col("review_date").isNotNull())

    # Додаємо колонку year_month для таблиці аналітики
    clean_df = clean_df.withColumn("year_month", date_format(col("review_date"), "yyyy-MM"))

    # 3. Запис у таблицю reviews_by_customer
    clean_df.select("customer_id", "review_date", "review_id", "product_id", "star_rating", "verified_purchase") \
        .write.format("org.apache.spark.sql.cassandra") \
        .options(table="reviews_by_customer", keyspace="amazon_reviews") \
        .mode("append").save()

    # 4. Запис у таблицю reviews_by_product
    clean_df.select("product_id", "star_rating", "review_id", "customer_id", "review_body", "review_date") \
        .write.format("org.apache.spark.sql.cassandra") \
        .options(table="reviews_by_product", keyspace="amazon_reviews") \
        .mode("append").save()

    # 5. Запис у таблицю reviews_for_analytics
    clean_df.select("year_month", "review_date", "review_id", "customer_id", "product_id", "star_rating", "verified_purchase") \
        .write.format("org.apache.spark.sql.cassandra") \
        .options(table="reviews_for_analytics", keyspace="amazon_reviews") \
        .mode("append").save()

    print("--- ETL COMPLETE: All tables loaded! ---")

except Exception as e:
    print(f"!!! ETL Failed with error: {e}")

finally:
    spark.stop()