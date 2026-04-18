from pyspark.sql import DataFrame

def load_to_cassandra(df: DataFrame, table_name: str, keyspace: str = "amazon_reviews"):
    df.write.format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace) \
        .mode("append") \
        .save()
    print(f"--- Data loaded into {keyspace}.{table_name} ---")