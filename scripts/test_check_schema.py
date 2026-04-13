from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckSchema").getOrCreate()
df = spark.read.parquet("/opt/spark/data/processed/reviews.parquet")
df.printSchema()
df.show(5)
spark.stop()