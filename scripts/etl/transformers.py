from pyspark.sql.functions import col, date_format
from pyspark.sql.types import IntegerType, DateType

def clean_reviews_data(df):
    """Очищення з кастом до DateType для сумісності з Cassandra 'date'"""
    return df.withColumn("review_date_cleaned", col("review_date").cast(DateType())) \
             .filter(col("review_date_cleaned").isNotNull()) \
             .drop("review_date") \
             .withColumnRenamed("review_date_cleaned", "review_date") \
             .filter(col("product_id").isNotNull())

def get_product_stats(df):
    # PK: (period, review_count, product_id)
    return df.withColumn("period", date_format(col("review_date"), "yyyy-MM")) \
        .groupBy("period", "product_id") \
        .count() \
        .withColumn("review_count", col("count").cast(IntegerType())) \
        .drop("count")

def get_customer_verified_stats(df):
    # PK: (period, review_count, customer_id)
    return df.withColumn("period", date_format(col("review_date"), "yyyy-MM")) \
        .groupBy("period", "customer_id") \
        .count() \
        .withColumn("review_count", col("count").cast(IntegerType())) \
        .drop("count")

def get_hater_stats(df):
    # PK: (period, bad_reviews_count, customer_id)
    return df.filter(col("star_rating") <= 2) \
        .withColumn("period", date_format(col("review_date"), "yyyy-MM")) \
        .groupBy("period", "customer_id") \
        .count() \
        .withColumn("bad_reviews_count", col("count").cast(IntegerType())) \
        .drop("count")

def get_backer_stats(df):
    # PK: (period, good_reviews_count, customer_id)
    return df.filter(col("star_rating") >= 4) \
        .withColumn("period", date_format(col("review_date"), "yyyy-MM")) \
        .groupBy("period", "customer_id") \
        .count() \
        .withColumn("good_reviews_count", col("count").cast(IntegerType())) \
        .drop("count")