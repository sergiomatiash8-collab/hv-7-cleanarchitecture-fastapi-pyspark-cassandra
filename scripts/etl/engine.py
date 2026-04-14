from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "AmazonReviewsETL"):
    """Створює та налаштовує Spark сесію.BREAKING DOWN LOADING TO CASSANDRA """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()