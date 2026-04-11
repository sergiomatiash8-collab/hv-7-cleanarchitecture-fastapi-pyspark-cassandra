import sys
import os

# Шлях до кореня
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.infrastructure.repositories.cassandra_repository import CassandraReviewRepository

def test_connection():
    print("🚀 Тестуємо CassandraReviewRepository...")
    repo = CassandraReviewRepository()
    try:
        # Перевіряємо метод отримання даних (зміни назву методу, якщо в класі вона інша)
        reviews = repo.get_by_product_id("B00000IGBF") 
        print(f"✅ Знайдено {len(reviews)} відгуків!")
    except Exception as e:
        print(f"❌ Помилка: {e}")

if __name__ == "__main__":
    test_connection()