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
    # 1. Створюємо тестові дані
    data = [
        ("1", "P1", 5, "Great!"),
        ("2", "P2", 2, "Bad"),
        (None, "P3", 4, "No ID") # Має відфільтруватися
    ]
    columns = ["review_id", "product_id", "star_rating", "review_body"]
    input_df = spark.createDataFrame(data, columns)

    # 2. Викликаємо нашу функцію
    result_df = transform_reviews(input_df)

    # 3. Перевіряємо результати
    results = result_df.collect()

    assert len(results) == 2  # Один запис без ID мав зникнути
    assert results[0]["priority"] == "High" # Рейтинг 5
    assert results[1]["priority"] == "Low"  # Рейтинг 2