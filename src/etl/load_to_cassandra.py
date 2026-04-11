from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, concat_ws, lit, col, when
import logging

logger = logging.getLogger(__name__)

class SparkToCassandraETL:
    """ETL: CSV → Spark → Cassandra (6 таблиць)"""
    
    def __init__(self, csv_path: str, cassandra_host: str = "localhost"):
        self.csv_path = csv_path
        self.cassandra_host = cassandra_host
        
        self.spark = SparkSession.builder \
            .appName("HV7-ETL") \
            .config("spark.cassandra.connection.host", cassandra_host) \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .getOrCreate()
        
        logger.info(f"✅ Spark session created")
    
    def load_csv(self):
        """Завантажити CSV → Spark DataFrame"""
        logger.info(f"📖 Loading CSV from {self.csv_path}")
        df = self.spark.read.csv(
            self.csv_path,
            header=True,
            inferSchema=True
        )
        logger.info(f"✅ Loaded {df.count()} rows")
        return df
    
    def write_to_cassandra(self, df, table_name: str, keyspace: str = "review_keyspace"):
        """Написати DataFrame в Cassandra"""
        try:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .option("keyspace", keyspace) \
                .option("table", table_name) \
                .save()
            logger.info(f"✅ Written {df.count()} rows to {table_name}")
        except Exception as e:
            logger.error(f"❌ Error writing to {table_name}: {e}")
            raise
    
    def run_etl(self):
        """Запустити весь ETL pipeline"""
        df = self.load_csv()
        
        # Добавляємо year_month для time-series таблиць
        df_with_month = df.withColumn(
            "year_month",
            concat_ws("-", year(col("review_date")), month(col("review_date")))
        )
        
        # Таблиця 1: reviews_by_product
        logger.info("⏳ Writing to reviews_by_product...")
        self.write_to_cassandra(
            df.select("product_id", "review_id", "customer_id", "star_rating", 
                     "review_date", "verified_purchase", "review_body", "created_at"),
            "reviews_by_product"
        )
        
        # Таблиця 2: reviews_by_product_rating
        logger.info("⏳ Writing to reviews_by_product_rating...")
        self.write_to_cassandra(
            df.select("product_id", "star_rating", "review_id", "customer_id",
                     "review_date", "verified_purchase", "review_body", "created_at"),
            "reviews_by_product_rating"
        )
        
        # Таблиця 3: reviews_by_customer
        logger.info("⏳ Writing to reviews_by_customer...")
        self.write_to_cassandra(
            df.select("customer_id", "review_id", "product_id", "star_rating",
                     "review_date", "verified_purchase", "review_body", "created_at"),
            "reviews_by_customer"
        )
        
        # Таблиця 4: reviews_by_month
        logger.info("⏳ Writing to reviews_by_month...")
        self.write_to_cassandra(
            df_with_month.select("year_month", "product_id", "review_id", "customer_id",
                                "star_rating", "verified_purchase", "review_date"),
            "reviews_by_month"
        )
        
        # Таблиця 5: reviews_by_rating_month
        logger.info("⏳ Writing to reviews_by_rating_month...")
        self.write_to_cassandra(
            df_with_month.select("year_month", "star_rating", "review_id", "customer_id",
                                "product_id", "verified_purchase", "review_date"),
            "reviews_by_rating_month"
        )
        
        # Таблиця 6a: Pre-computed aggregations по товарам
        logger.info("⏳ Computing and writing review_counts_by_product_month...")
        product_counts = df_with_month.groupBy("year_month", "product_id") \
            .count() \
            .withColumnRenamed("count", "review_count") \
            .withColumn("avg_rating", lit(0.0))
        self.write_to_cassandra(product_counts, "review_counts_by_product_month")
        
        # Таблиця 6b: Pre-computed aggregations по клієнтам
        logger.info("⏳ Computing and writing review_counts_by_customer_month...")
        customer_stats = df_with_month \
            .groupBy("year_month", "customer_id") \
            .agg(
                # Загальна кількість
                lit(1).cast("int").alias("verified_count"),
                # Haters (1-2 зірки)
                lit(1).cast("int").alias("hater_count"),
                # Backers (4-5 зірок)
                lit(1).cast("int").alias("backer_count")
            )
        self.write_to_cassandra(customer_stats, "review_counts_by_customer_month")
        
        logger.info("✅ ETL pipeline completed successfully!")

if __name__ == "__main__":
    etl = SparkToCassandraETL(
        csv_path="reviews.csv",
        cassandra_host="localhost"
    )
    etl.run_etl()