import uuid
from cassandra.cluster import Cluster
from src.domain.entities import Review
from src.domain.interfaces import IReviewRepository

class CassandraReviewRepository(IReviewRepository):
    def __init__(self):
        # Підключення до сервісу cassandra_dev у мережі Docker
        # Використовуємо ключовий простір 'reviews_keyspace'
        self.cluster = Cluster(['cassandra_dev'])
        self.session = self.cluster.connect('reviews_keyspace')

    def save(self, review: Review):
        """
        Зберігає сутність відгуку одночасно у три таблиці для швидкої вибірки.
        Це забезпечує роботу API без ALLOW FILTERING.
        """
        queries = {
            "by_product": """
                INSERT INTO reviews_by_product (product_id, created_at, review_id, customer_id, star_rating, review_body)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
            "by_rating": """
                INSERT INTO reviews_by_product_rating (product_id, star_rating, created_at, review_id, customer_id, review_body)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
            "by_customer": """
                INSERT INTO reviews_by_customer (customer_id, created_at, review_id, product_id, star_rating, review_body)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
        }

        # Конвертуємо ID у формат UUID для Cassandra
        review_uuid = uuid.UUID(review.review_id) if isinstance(review.review_id, str) else review.review_id
        
        # 1. Запис для Return all reviews for specified product_id
        self.session.execute(queries["by_product"], (
            review.product_id, review.created_at, review_uuid, 
            review.customer_id, review.star_rating, review.review_body
        ))
        
        # 2. Запис для Return reviews by product_id AND star_rating
        self.session.execute(queries["by_rating"], (
            review.product_id, review.star_rating, review.created_at, 
            review_uuid, review.customer_id, review.review_body
        ))
        
        # 3. Запис для Return all reviews for specified customer_id
        self.session.execute(queries["by_customer"], (
            review.customer_id, review.created_at, review_uuid, 
            review.product_id, review.star_rating, review.review_body
        ))

        print(f"✅ [Cassandra] Review {review.review_id} synced to all redundant tables.")

    def get_by_product(self, product_id: str) -> list[Review]:
        """Отримує всі відгуки для конкретного продукту."""
        query = "SELECT * FROM reviews_by_product WHERE product_id = %s"
        rows = self.session.execute(query, [product_id])
        return self._map_rows_to_entities(rows)

    def get_by_product_and_rating(self, product_id: str, rating: int) -> list[Review]:
        """Отримує відгуки для продукту з конкретним рейтингом."""
        query = "SELECT * FROM reviews_by_product_rating WHERE product_id = %s AND star_rating = %s"
        rows = self.session.execute(query, (product_id, rating))
        return self._map_rows_to_entities(rows)

    def get_by_customer(self, customer_id: str) -> list[Review]:
        """Отримує всі відгуки конкретного покупця."""
        query = "SELECT * FROM reviews_by_customer WHERE customer_id = %s"
        rows = self.session.execute(query, [customer_id])
        return self._map_rows_to_entities(rows)

    def get_all(self) -> list[Review]:
        """Реалізація абстрактного методу інтерфейсу. Повертає лімітований список."""
        query = "SELECT * FROM reviews_by_product LIMIT 10"
        rows = self.session.execute(query)
        return self._map_rows_to_entities(rows)

    def _map_rows_to_entities(self, rows) -> list[Review]:
        """Допоміжний метод для мапінгу результатів БД у сутності Domain."""
        results = []
        for row in rows:
            results.append(Review(
                review_id=str(row.review_id),
                product_id=row.product_id,
                customer_id=row.customer_id,
                star_rating=row.star_rating,
                review_body=row.review_body,
                created_at=row.created_at
            ))
        return results

    def close(self):
        """Закриття з'єднання з кластером."""
        self.cluster.shutdown()