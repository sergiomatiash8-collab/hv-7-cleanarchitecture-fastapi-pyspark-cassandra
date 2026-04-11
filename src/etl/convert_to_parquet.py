import polars as pl
import os

INPUT_CSV = "data/raw/amazon_reviews.csv" 
OUTPUT_FOLDER = "data/interim/reviews_parquet"

def convert():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Помилка: Файл {INPUT_CSV} не знайдено!")
        return

    print("⏳ Читаємо CSV...")
    # Читаємо файл (тепер без separator, бо там кома)
    df = pl.read_csv(INPUT_CSV, ignore_errors=True)

    print("⏳ Обробляємо типи та дату...")
    df = df.with_columns([
        # Конвертуємо дату в формат datetime для нашого поля created_at
        pl.col("review_date").str.to_datetime("%Y-%m-%d").alias("created_at"),
        # Гарантуємо, що рейтинг — це число
        pl.col("star_rating").cast(pl.Int32)
    ])

    # Відбираємо тільки ті колонки, які потрібні для Domain Entity та бази
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
    
    print("✅ Готово! Перевір папку.")

if __name__ == "__main__":
    convert()