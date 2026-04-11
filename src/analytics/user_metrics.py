from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class UserAnalytics:
    @staticmethod
    def get_haters(df: DataFrame) -> DataFrame:
        """Користувачі, які ставлять найбільше 1 зірки"""
        return df.filter(F.col("star_rating") == 1) \
            .groupBy("customer_id") \
            .agg(F.count("review_id").alias("total_reviews"))

    @staticmethod
    def get_backers(df: DataFrame) -> DataFrame:
        """Користувачі, які ставлять найбільше 5 зірок"""
        return df.filter(F.col("star_rating") == 5) \
            .groupBy("customer_id") \
            .agg(F.count("review_id").alias("total_reviews"))