from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class ProductAnalytics:
    """
    Клас для аналітики товарів.
    Відповідає за розрахунок популярності та рейтингів продуктів.
    """
    
    @staticmethod
    def get_top_products(df: DataFrame, limit: int = 10) -> DataFrame:
        """
        Знаходить найпопулярніші товари за кількістю відгуків.
        """
        print(f"🔝 [Analytics] Calculating Top {limit} Products...")
        
        return df.groupBy("product_id") \
            .agg(F.count("review_id").alias("review_count")) \
            .orderBy(F.col("review_count").desc()) \
            .limit(limit)