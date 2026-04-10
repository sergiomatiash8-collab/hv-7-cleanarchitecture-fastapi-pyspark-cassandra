import polars as pl
import os


INPUT_CSV = "data/raw/amazon_reviews.csv" 
OUTPUT_FOLDER = "data/interim/reviews_parquet"

def convert():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Помилка: Файл {INPUT_CSV} не знайдено!")
        return

    print("⏳ Читаємо CSV...")

    df = pl.read_csv(INPUT_CSV, ignore_errors=True)
    
 
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    print(f"⏳ Записуємо Parquet у {OUTPUT_FOLDER}...")
    df.write_parquet(f"{OUTPUT_FOLDER}/reviews.parquet")
    
    print("Done")

if __name__ == "__main__":
    convert()