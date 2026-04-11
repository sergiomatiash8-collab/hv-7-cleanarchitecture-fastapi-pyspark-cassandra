from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class UserAnalytics:
    """
    Клас для проведення аналітики користувачів за допомогою PySpark.
    """
    
    @staticmethod
    def get_haters(df: DataFrame) -> DataFrame:
        """
        Знаходить користувачів, які ставлять тільки низькі оцінки (<= 2).
        Пункт ТЗ: Аналітика поведінки.
        """
        print("🔍 [Analytics] Calculating Haters metric...")
        
        return df.groupBy("customer_id") \
            .agg(
                F.max("star_rating").alias("max_rating"),
                F.count("review_id").alias("total_reviews")
            ) \
            .filter("max_rating <= 2") \
            .select("customer_id", "total_reviews", "max_rating")

    # Сюди ми наступним кроком додамо метод get_backers