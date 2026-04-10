import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_data():
    # Define the base path and configure HADOOP_HOME for the environment
    base_path = Path(__file__).parent.parent.parent
    hadoop_home = str(base_path / "hadoop")
    os.environ['HADOOP_HOME'] = hadoop_home
    
    # 1. Initialize Spark Session with Cassandra Connector
    spark = SparkSession.builder \
        .appName("ReviewsETL") \
        .config("spark.cassandra.connection.host", "cassandra_dev") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .getOrCreate()

    print(f"HADOOP_HOME set to: {hadoop_home}")
    print("Reading interim Parquet data...")
    
    # 2. Read processed data from the specific Parquet file
    try:
        df = spark.read.parquet("data/interim/reviews_parquet/reviews.parquet")
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return

    # 3. Select and rename columns to match Cassandra schema
    # Rename 'review_date' to 'created_at' to match your database
    df_filtered = df.select(
        "review_id",
        "product_id",
        "customer_id",
        "star_rating",
        "review_body",
        F.col("review_date").alias("created_at")
    )

    # 4. Define target tables for the Cassandra data model
    tables = ["reviews", "reviews_by_product", "reviews_by_customer"]

    # 5. Iteratively write the filtered DataFrame into each Cassandra table
    for table_name in tables:
        print(f"Loading data into {table_name}...")
        try:
            df_filtered.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=table_name, keyspace="reviews_keyspace") \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Could not load into {table_name}: {e}")

    print("ETL Process complete: Data loaded into Cassandra.")
    spark.stop()

if __name__ == "__main__":
    load_data()