import pytest
from pyspark.sql import SparkSession
from etl.transformers import clean_reviews_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("ETLTests") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

def test_clean_reviews_data_removes_duplicates(spark):
    # Створюємо тестові дані з дублікатом
    data = [
        ("R1", 101, "P1", 5, "Great!", "2026-04-14", "Y"),
        ("R1", 101, "P1", 5, "Great!", "2026-04-14", "Y"), # Дублікат
        ("R2", 102, "P2", 4, "Good", "2026-04-15", "N")
    ]
    columns = ["review_id", "customer_id", "product_id", "star_rating", "review_body", "review_date", "verified_purchase"]
    df = spark.createDataFrame(data, columns)

    # Викликаємо нашу функцію
    cleaned_df = clean_reviews_data(df)

    # Перевіряємо результат: має залишитися 2 рядки
    assert cleaned_df.count() == 2