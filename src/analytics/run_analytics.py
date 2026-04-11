import os
import sys
from pyspark.sql import SparkSession
from src.analytics.user_metrics import UserAnalytics
from src.analytics.product_metrics import ProductAnalytics

def main():
    # 1. Налаштовуємо HADOOP_HOME прямо в коді для Windows
    project_root = os.getcwd()
    hadoop_path = os.path.join(project_root, "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_path
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_path, "bin")

    # 2. Створюємо Spark-сесію
    # ВИДАЛЕНО Extensions, щоб уникнути помилок несумісності методів у Spark 3.5
    spark = SparkSession.builder \
        .appName("ReviewAnalyticsRunner") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # 3. Читаємо дані з Cassandra
    print(" [Spark] Reading data from Cassandra (review_keyspace.reviews)...")
    try:
        df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="reviews", keyspace="review_keyspace") \
            .load()

        # Перевірка на наявність даних
        if df.isEmpty():
            print(" [Spark] No data found in Cassandra! Перевір, чи завантажені дані.")
            return

        # 4. Запуск аналітики користувачів
        print("\n [Analytics] Processing User Metrics...")
        haters = UserAnalytics.get_haters(df)
        print("TOP 5 HATERS (1-star):")
        haters.orderBy("total_reviews", ascending=False).show(5)

        backers = UserAnalytics.get_backers(df)
        print(" TOP 5 BACKERS (5-stars):")
        backers.orderBy("total_reviews", ascending=False).show(5)

        # 5. Запуск аналітики товарів
        print("\n [Analytics] Processing Product Metrics...")
        top_products = ProductAnalytics.get_top_products(df, limit=5)
        print("TOP 5 PRODUCTS BY REVIEWS:")
        top_products.show()

    except Exception as e:
        print(f" [Error] Analytics failed: {e}")

    finally:
        spark.stop()
        print("\n [Spark] Analytics session closed.")

if __name__ == "__main__":
    main()