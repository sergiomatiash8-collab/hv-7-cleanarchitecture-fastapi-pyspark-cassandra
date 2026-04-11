from pyspark.sql import SparkSession
from src.analytics.user_metrics import UserAnalytics
from src.analytics.product_metrics import ProductAnalytics

def main():
    # 1. Створюємо Spark-сесію
    spark = SparkSession.builder \
        .appName("ReviewAnalyticsRunner") \
        .get_session()

    # 2. Читаємо дані з Parquet (це наше джерело істини для аналітики)
    # Шлях має відповідати тому, куди твій ETL зберіг Parquet
    path_to_parquet = "data/interim/reviews_parquet"
    print(f"📥 [Spark] Reading data from {path_to_parquet}...")
    df = spark.read.parquet(path_to_parquet)

    # 3. Запуск аналітики користувачів
    haters = UserAnalytics.get_haters(df)
    print("💀 TOP 5 HATERS:")
    haters.orderBy("total_reviews", ascending=False).show(5)

    backers = UserAnalytics.get_backers(df)
    print("💎 TOP 5 BACKERS:")
    backers.orderBy("total_reviews", ascending=False).show(5)

    # 4. Запуск аналітики товарів
    top_products = ProductAnalytics.get_top_products(df, limit=5)
    print("🔥 TOP 5 PRODUCTS BY REVIEWS:")
    top_products.show()

    spark.stop()

if __name__ == "__main__":
    main()