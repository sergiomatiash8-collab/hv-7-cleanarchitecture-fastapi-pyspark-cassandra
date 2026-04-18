import os
import sys


sys.path.insert(0, '/opt/spark')

from etl.engine import get_spark_session
from etl.transformers import (
    clean_reviews_data, get_product_stats, 
    get_customer_verified_stats, get_hater_stats, get_backer_stats
)
from etl.loaders import load_to_cassandra

def run_main_pipeline():
    data_path = "/opt/spark/data/processed/reviews.parquet"
    
    print("\n" + "="*50)
    print(f"STARTING ETL PIPELINE")
    print(f"Data path: {data_path}")
    print("="*50 + "\n")
    
    if not os.path.exists(data_path):
        print(f" ERROR: File not found at {data_path}")
        sys.exit(1)

    print("--- Initializing Spark Session ---")
    spark = get_spark_session()
    
    
    spark.conf.set("spark.sql.ansi.enabled", "false")
    
    print("--- Reading Parquet ---")
    raw_df = spark.read.parquet(data_path)
    
    print("--- Transforming Data ---")
    
    clean_df = clean_reviews_data(raw_df).cache()
    
    print("--- Loading: reviews_by_product ---")
    load_to_cassandra(
        clean_df.select("product_id", "star_rating", "review_id", "customer_id", "review_body", "review_date"),
        "reviews_by_product"
    )

    print("--- Loading: reviews_by_customer ---")
    load_to_cassandra(
        clean_df.select("customer_id", "review_date", "review_id", "product_id", "star_rating", "verified_purchase"),
        "reviews_by_customer"
    )
    
    print("--- Loading: product_stats_by_period ---")
    load_to_cassandra(get_product_stats(clean_df), "product_stats_by_period")

    print("--- Loading: customer_verified_stats_by_period ---")
    load_to_cassandra(get_customer_verified_stats(clean_df), "customer_verified_stats_by_period")

    print("--- Loading: hater_stats_by_period ---")
    load_to_cassandra(get_hater_stats(clean_df), "hater_stats_by_period")

    print("--- Loading: backer_stats_by_period ---")
    load_to_cassandra(get_backer_stats(clean_df), "backer_stats_by_period")
    
    print("\n" + "="*50)
    print(" SUCCESS: ALL DATA LOADED TO CASSANDRA")
    print("="*50 + "\n")
    
    clean_df.unpersist()
    spark.stop()

if __name__ == "__main__":
    run_main_pipeline()