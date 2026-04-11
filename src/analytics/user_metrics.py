from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class UserAnalytics:
    """
    Клас для проведення аналітики користувачів за допомогою PySpark.
    Відокремлений від ETL шару згідно з принципами Clean Architecture.
    """
    
    @staticmethod
    def get_haters(df: DataFrame) -> DataFrame:
        """
        Знаходить 'Хейтерів': користувачів, які нікому не поставили більше 2 зірок.
        Логіка: якщо МАКСИМАЛЬНИЙ рейтинг користувача <= 2, то всі його оцінки низькі.
        """
        print("🔍 [Analytics] Calculating Haters metric...")
        
        return df.groupBy("customer_id") \
            .agg(
                F.max("star_rating").alias("max_rating"),
                F.count("review_id").alias("total_reviews")
            ) \
            .filter("max_rating <= 2") \
            .select("customer_id", "total_reviews", "max_rating")

    @staticmethod
    def get_backers(df: DataFrame) -> DataFrame:
        """
        Знаходить 'Бекерів': лояльних користувачів, які ставлять ТІЛЬКИ 5 зірок.
        Логіка: якщо МІНІМАЛЬНИЙ рейтинг користувача == 5, то у нього немає оцінок нижче.
        """
        print("💎 [Analytics] Calculating Backers metric...")
        
        return df.groupBy("customer_id") \
            .agg(
                F.min("star_rating").alias("min_rating"),
                F.count("review_id").alias("total_reviews")
            ) \
            .filter("min_rating == 5") \
            .select("customer_id", "total_reviews")