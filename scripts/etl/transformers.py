from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, date_format, trim, count

def clean_reviews_data(df: DataFrame) -> DataFrame:
    return df.withColumn("review_date", to_date(trim(col("review_date")), "yyyy-MM-dd")) \
             .withColumn("star_rating", col("star_rating").cast("int")) \
             .withColumn("verified_purchase", col("verified_purchase").cast("string")) \
             .filter(col("review_date").isNotNull()) \
             .dropDuplicates(["review_id"]) \
             .withColumn("year_month", date_format(col("review_date"), "yyyy-MM"))

def get_product_stats(df: DataFrame) -> DataFrame:
    return df.groupBy("year_month", "product_id") \
             .agg(count("review_id").alias("review_count")) \
             .select(col("year_month").alias("period"), "review_count", "product_id")

def get_customer_verified_stats(df: DataFrame) -> DataFrame:
    return df.filter(col("verified_purchase") == "Y") \
             .groupBy("year_month", "customer_id") \
             .agg(count("review_id").alias("review_count")) \
             .select(col("year_month").alias("period"), "review_count", "customer_id")

def get_hater_stats(df: DataFrame) -> DataFrame:
    return df.filter(col("star_rating").isin(1, 2)) \
             .groupBy("year_month", "customer_id") \
             .agg(count("review_id").alias("bad_reviews_count")) \
             .select(col("year_month").alias("period"), "bad_reviews_count", "customer_id")

def get_backer_stats(df: DataFrame) -> DataFrame:
    return df.filter(col("star_rating").isin(4, 5)) \
             .groupBy("year_month", "customer_id") \
             .agg(count("review_id").alias("good_reviews_count")) \
             .select(col("year_month").alias("period"), "good_reviews_count", "customer_id")