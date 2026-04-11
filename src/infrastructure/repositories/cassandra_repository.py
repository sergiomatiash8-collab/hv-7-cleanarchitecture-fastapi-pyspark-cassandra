import os
from cassandra.cluster import Cluster
from src.domain.entities import Review
from src.domain.interfaces import IReviewRepository

class CassandraReviewRepository(IReviewRepository):
    def __init__(self):
        # Використовуємо 127.0.0.1 для локального запуску на Windows
        host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
        
        try:
            self.cluster = Cluster([host], port=9042)
            # ВАЖЛИВО: У твоєму SQL простір названо review_keyspace (в однині)
            self.session = self.cluster.connect('review_keyspace')
            self._prepare_statements()
            print(f"🚀 [Cassandra] Connected to {host}")
        except Exception as e:
            print(f"❌ [Cassandra] Connection error: {e}")
            raise

    def _prepare_statements(self):
        """Готуємо запити під твою єдину таблицю reviews."""
        from cassandra import ConsistencyLevel # Додай цей імпорт вгорі файлу або тут

        self.prep_insert = self.session.prepare("""
            INSERT INTO reviews (review_id, product_id, customer_id, star_rating, review_body, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        
        # Створюємо запит на вибірку
        self.prep_by_product = self.session.prepare("""
            SELECT * FROM reviews WHERE product_id = ? ALLOW FILTERING
        """)
        
        # ФІКС: Встановлюємо мінімальну консистентність, щоб не було ReadFailure
        self.prep_by_product.consistency_level = ConsistencyLevel.ONE
        
        self.prep_get_all = self.session.prepare("SELECT * FROM reviews LIMIT 50")

    def save(self, review: Review):
        """Збереження сутності в базу."""
        try:
            self.session.execute(self.prep_insert, (
                str(review.review_id),
                review.product_id,
                review.customer_id,
                review.star_rating,
                review.review_body,
                review.created_at
            ))
            print(f"✅ [Cassandra] Review {review.review_id} saved.")
        except Exception as e:
            print(f"❌ [Cassandra] Save error: {e}")

    def get_by_product(self, product_id: str) -> list[Review]:
        """Отримання відгуків за product_id."""
        rows = self.session.execute(self.prep_by_product, [product_id])
        return self._map_rows_to_entities(rows)

    def get_all(self) -> list[Review]:
        """Повертає список відгуків (ліміт 50)."""
        rows = self.session.execute(self.prep_get_all)
        return self._map_rows_to_entities(rows)

    def get_by_customer(self, customer_id: str) -> list[Review]:
        """Пошук за клієнтом (теж потребує фільтрації)."""
        query = "SELECT * FROM reviews WHERE customer_id = %s ALLOW FILTERING"
        rows = self.session.execute(query, [customer_id])
        return self._map_rows_to_entities(rows)

    def _map_rows_to_entities(self, rows) -> list[Review]:
        """Конвертація рядків БД у об'єкти Review."""
        results = []
        for row in rows:
            results.append(Review(
                review_id=row.review_id,
                product_id=row.product_id,
                customer_id=row.customer_id,
                star_rating=row.star_rating,
                review_body=row.review_body,
                created_at=row.created_at
            ))
        return results

    def close(self):
        """Закриття з'єднання."""
        if not self.cluster.is_shutdown:
            self.cluster.shutdown()
            print("🔌 [Cassandra] Connection closed.")