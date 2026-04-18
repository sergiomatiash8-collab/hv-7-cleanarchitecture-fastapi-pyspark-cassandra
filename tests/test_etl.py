import pytest
from pyspark.sql import SparkSession
from etl_process import transform_reviews

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("PyTest-Spark") \
        .getOrCreate()

def test_transform_reviews_priority_logic(spark):
    # 1. Create test data
    data = [
        ("1", "P1", 5, "Great!"),
        ("2", "P2", 2, "Bad"),
        (None, "P3", 4, "No ID") # Should be filtered out
    ]
    columns = ["review_id", "product_id", "star_rating", "review_body"]
    input_df = spark.createDataFrame(data, columns)

    # 2. Call the function
    result_df = transform_reviews(input_df)

    # 3. Verify results
    results = result_df.collect()

    assert len(results) == 2  
    assert results[0]["priority"] == "High" # Rating 5
    assert results[1]["priority"] == "Low"  # Rating 2