from pyspark.sql import SparkSession

def load_to_cassandra(df):
    """
    Завантажує DataFrame у три різні таблиці Cassandra для забезпечення 
    швидкої фільтрації на рівні API.
    """
    keyspace = "reviews_keyspace"
    tables = [
        "reviews_by_product",
        "reviews_by_product_rating",
        "reviews_by_customer"
    ]

    for table_name in tables:
        print(f"⏳ [Spark] Loading data into {table_name}...")
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table_name, keyspace=keyspace) \
            .mode("append") \
            .save()
        print(f"✅ [Spark] Table {table_name} loaded successfully.")

# Приклад виклику у твоєму main ETL скрипті:
# load_to_cassandra(final_transformed_df)