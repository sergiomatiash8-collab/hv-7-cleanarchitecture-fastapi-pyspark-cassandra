from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("FixParquet").getOrCreate()

    # ВКАЗУЄМО ШЛЯХ ВІДНОСНО КОНТЕЙНЕРА
    csv_path = "/opt/spark/data/raw/amazon_reviews.csv" 
    output_path = "/opt/spark/data/processed/reviews.parquet"

    df = spark.read.option("header", "true") \
                   .option("sep", ",") \
                   .option("inferSchema", "true") \
                   .csv(csv_path)

    df.write.mode("overwrite").parquet(output_path)
    
    print("="*50)
    print("КОЛОНКИ ПЕРЕВІРЕНО:")
    print(df.columns)
    print("="*50)
    
    spark.stop()

if __name__ == "__main__":
    main()