from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, date_format, trim


#test found mistake

def clean_reviews_data(df: DataFrame) -> DataFrame:
    """Очищає сирі дані та додає технічні колонки (year_month)"""
    return df.withColumn("review_date", to_date(trim(col("review_date")), "yyyy-MM-dd")) \
             .withColumn("star_rating", col("star_rating").cast("int")) \
             .withColumn("verified_purchase", col("verified_purchase").cast("string")) \
             .filter(col("review_date").isNotNull()) \
             .dropDuplicates(["review_id"]) \
             .withColumn("year_month", date_format(col("review_date"), "yyyy-MM"))