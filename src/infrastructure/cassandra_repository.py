# src/infrastructure/cassandra_repository.py
from cassandra.cluster import Cluster, Session
from cassandra.io.geventreactor import GeventConnection
from typing import Optional
from ..domain.repositories import ReviewRepository
from ..domain.entities import Review, ProductReviews, CustomerReviews
from datetime import date

class CassandraReviewRepository(ReviewRepository):
    """Реалізація репозиторію через Cassandra (SRP - тільки робота з БД)"""
    
    def __init__(self, session: Session):
        self._session = session
    
    def get_by_product(self, product_id: str) -> ProductReviews:
        query = "SELECT * FROM reviews_by_product WHERE product_id = %s"
        rows = self._session.execute(query, [product_id])
        
        reviews = [self._row_to_review(row) for row in rows]
        return ProductReviews(product_id=product_id, reviews=reviews)
    
    def get_by_product_and_rating(self, product_id: str, rating: int) -> ProductReviews:
        # Отримуємо всі відгуки продукту і фільтруємо на стороні Python
        all_reviews = self.get_by_product(product_id)
        filtered = [r for r in all_reviews.reviews if r.star_rating == rating]
        return ProductReviews(product_id=product_id, reviews=filtered)
    
    def get_by_customer(self, customer_id: str) -> CustomerReviews:
        query = "SELECT * FROM reviews_by_customer WHERE customer_id = %s"
        rows = self._session.execute(query, [customer_id])
        
        reviews = [self._row_to_review(row) for row in rows]
        return CustomerReviews(customer_id=customer_id, reviews=reviews)
    
    @staticmethod
    def _row_to_review(row) -> Review:
        """Маппінг row Cassandra в domain entity"""
        return Review(
            review_id=row.review_id,
            product_id=row.product_id,
            customer_id=getattr(row, 'customer_id', ''),
            star_rating=row.star_rating,
            review_date=row.review_date,
            review_body=getattr(row, 'review_body', None)
        )