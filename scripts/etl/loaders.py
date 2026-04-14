from pyspark.sql import DataFrame

def load_to_cassandra(df: DataFrame, table_name: str, keyspace: str = "amazon_reviews"):
    """
    Універсальна функція для завантаження DataFrame у вказану таблицю Cassandra BREAKING DOWN LOAD TO CASSANDA
    """
    df.write.format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace) \
        .mode("append") \
        .save()
    print(f"--- Data successfully loaded into {keyspace}.{table_name} ---")