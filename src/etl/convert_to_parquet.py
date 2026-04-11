import polars as pl
import os
from datetime import datetime

INPUT_CSV = "data/raw/amazon_reviews.csv" 
OUTPUT_FOLDER = "data/interim/reviews_parquet"

def convert():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Помилка: Файл {INPUT_CSV} не знайдено!")
        return

    print("⏳ Читаємо CSV та приводимо типи...")

    # Читаємо з явною схемою (або конвертуємо після), щоб star_rating був INT
    df = pl.read_csv(INPUT_CSV, ignore_errors=True)

    # 1. Перейменовуємо колонки під наш стандарт (Unified Naming)
    # Приклад для Amazon CSV: review_id, product_id, customer_id, star_rating, review_body, review_date
    # Ми міняємо review_date -> created_at
    
    df = df.with_columns([
        pl.col("star_rating").cast(pl.Int32),  # Важливо для аналітики!
        pl.col("review_date").str.to_datetime("%Y-%m-%d").alias("created_at") # Створюємо created_at
    ])

    # 2. Відбираємо тільки потрібні колонки (як у нашому Domain Entity)
    df = df.select([
        "review_id",
        "product_id",
        "customer_id",
        "star_rating",
        "review_body",
        "created_at"
    ])

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    print(f"⏳ Записуємо Parquet у {OUTPUT_FOLDER}...")
    df.write_parquet(f"{OUTPUT_FOLDER}/reviews.parquet")
    
    print("✅ Done: Дані готові для Spark та Cassandra!")

if __name__ == "__main__":
    convert()