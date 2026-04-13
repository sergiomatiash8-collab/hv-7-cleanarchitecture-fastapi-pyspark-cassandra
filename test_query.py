from gevent import monkey
monkey.patch_all()

from cassandra.cluster import Cluster
from cassandra.io.geventreactor import GeventConnection

def test_fetch_product_reviews():
    try:
        # 1. Підключення
        cluster = Cluster(['127.0.0.1'], port=9042)
        cluster.connection_class = GeventConnection
        session = cluster.connect('amazon_reviews')
        
        # 2. Отримуємо один ID продукту для тесту
        # Спочатку просто візьмемо будь-який 1 запис, щоб побачити ID
        sample = session.execute("SELECT product_id FROM reviews_by_product LIMIT 1").one()
        
        if not sample:
            print("❌ Таблиця reviews_by_product порожня!")
            return

        test_id = sample.product_id
        print(f"🔎 Тестуємо пошук для продукту ID: {test_id}")

        # 3. Виконуємо цільовий запит
        query = f"SELECT * FROM reviews_by_product WHERE product_id = '{test_id}'"
        rows = session.execute(query)

        print(f"\n✅ Результати пошуку:")
        for i, row in enumerate(rows):
            print(f"{i+1}. Rating: {row.star_rating} | Date: {row.review_date} | ID: {row.review_id}")
        
        cluster.shutdown()
        
    except Exception as e:
        print(f"❌ Помилка під час запиту: {e}")

if __name__ == "__main__":
    test_fetch_product_reviews()