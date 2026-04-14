from cassandra.cluster import Cluster, Session
from cassandra.io.geventreactor import GeventConnection
from typing import Optional
from datetime import date

# Перехід на абсолютні імпорти
from src.domain.repositories import ReviewRepository
from src.domain.entities import Review, ProductReviews, CustomerReviews

class CassandraReviewRepository(ReviewRepository):
    """
    Конкретна реалізація інтерфейсу ReviewRepository для Apache Cassandra.
    
    Відповідальність: Інкапсуляція CQL-запитів (Cassandra Query Language) та 
    забезпечення зв'язку між фізичною схемою таблиць і доменною моделлю.
    """
    
    def __init__(self, session: Session):
        """
        Ініціалізація через активну сесію Cassandra.
        """
        self._session = session
    
    def get_by_product(self, product_id: str) -> ProductReviews:
        """
        Отримує відгуки з таблиці, оптимізованої для пошуку за продуктом.
        """
        query = "SELECT * FROM reviews_by_product WHERE product_id = %s"
        rows = self._session.execute(query, [product_id])
        
        reviews = [self._row_to_review(row) for row in rows]
        return ProductReviews(product_id=product_id, reviews=reviews)
    
    def get_by_product_and_rating(self, product_id: str, rating: int) -> ProductReviews:
        """
        Отримує відгуки продукту та фільтрує їх за рейтингом (In-memory filtering).
        """
        all_reviews = self.get_by_product(product_id)
        filtered = [r for r in all_reviews.reviews if r.star_rating == rating]
        return ProductReviews(product_id=product_id, reviews=filtered)
    
    def get_by_customer(self, customer_id: str) -> CustomerReviews:
        """
        Отримує відгуки з денормалізованої таблиці reviews_by_customer.
        """
        query = "SELECT * FROM reviews_by_customer WHERE customer_id = %s"
        rows = self._session.execute(query, [customer_id])
        
        reviews = [self._row_to_review(row) for row in rows]
        return CustomerReviews(customer_id=customer_id, reviews=reviews)
    
    @staticmethod
    def _row_to_review(row) -> Review:
        """
        Допоміжний метод-мапер (Data Mapper).
        """
        return Review(
            review_id=row.review_id,
            product_id=row.product_id,
            customer_id=getattr(row, 'customer_id', ''),
            star_rating=row.star_rating,
            review_date=row.review_date,
            review_body=getattr(row, 'review_body', None)
        )