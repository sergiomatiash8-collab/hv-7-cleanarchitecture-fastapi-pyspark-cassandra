from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "AmazonReviewsETL"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .getOrCreate()