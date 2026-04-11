from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class ProductAnalytics:
    """
    Клас для аналітики товарів.
    Відповідає за розрахунок популярності та рейтингів продуктів.
    """
    
    @staticmethod
    def get_product_summary(df: DataFrame) -> DataFrame:
        """
        Розраховує загальну статистику для кожного товару:
        кількість відгуків та середній рейтинг.
        """
        print("📊 [Analytics] Calculating product summary (count & avg rating)...")
        
        return df.groupBy("product_id") \
            .agg(
                F.count("review_id").alias("review_count"),
                F.round(F.avg("star_rating"), 2).alias("avg_rating")
            )

    @staticmethod
    def get_top_products(df: DataFrame, limit: int = 10) -> DataFrame:
        """
        Знаходить найпопулярніші товари за кількістю відгуків.
        """
        print(f"🔝 [Analytics] Fetching Top {limit} Products by review count...")
        
        summary = ProductAnalytics.get_product_summary(df)
        return summary.orderBy(F.col("review_count").desc()).limit(limit)

    @staticmethod
    def get_highest_rated_products(df: DataFrame, min_reviews: int = 5) -> DataFrame:
        """
        Знаходить товари з найвищим рейтингом, де кількість відгуків >= min_reviews.
        """
        print(f"⭐ [Analytics] Fetching Highest Rated Products (min {min_reviews} reviews)...")
        
        summary = ProductAnalytics.get_product_summary(df)
        return summary.filter(F.col("review_count") >= min_reviews) \
            .orderBy(F.col("avg_rating").desc(), F.col("review_count").desc())