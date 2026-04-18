from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "AmazonReviewsETL"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()