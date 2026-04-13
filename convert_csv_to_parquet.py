import pandas as pd
import os

def convert():
    # Шляхи до файлів
    input_path = "data/raw/amazon_reviews.csv"
    output_dir = "data/processed"
    output_path = os.path.join(output_dir, "reviews.parquet")
    
    # Створюємо папку для результату, якщо її нема
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"🚀 Читаю файл з {input_path}...")
    
    try:
        # Amazon Dataset зазвичай використовує табуляцію (\t)
        df = pd.read_csv(input_path, sep='\t', on_bad_lines='skip', low_memory=False)
        
        print("📊 Дані завантажено. Конвертую в Parquet...")
        df.to_parquet(output_path, engine='pyarrow', index=False)
        
        print(f"✅ Успіх! Файл збережено: {output_path}")
        print(f"📏 Розмір DataFrame: {df.shape[0]} рядків")
        
    except FileNotFoundError:
        print(f"❌ Помилка: Файл не знайдено за шляхом {input_path}")
    except Exception as e:
        print(f"❌ Виникла помилка: {e}")

if __name__ == "__main__":
    convert()