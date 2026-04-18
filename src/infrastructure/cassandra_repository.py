from cassandra.cluster import Session
from typing import List, Optional
from src.domain.repositories import ReviewRepository
from src.domain.entities import (
    Review, ProductReviews, CustomerReviews, 
    TopReviewedProduct, TopCustomer, TopHater, TopBacker
)

class CassandraReviewRepository(ReviewRepository):
    def __init__(self, session: Session):
        self._session = session

    def get_by_product(self, product_id: str) -> ProductReviews:
        query = "SELECT * FROM reviews_by_product WHERE product_id = %s"
        rows = self._session.execute(query, [product_id])
        return ProductReviews(product_id=product_id, reviews=[self._row_to_review(row) for row in rows])

    def get_by_product_and_rating(self, product_id: str, rating: int) -> ProductReviews:
        query = "SELECT * FROM reviews_by_product WHERE product_id = %s AND star_rating = %s"
        rows = self._session.execute(query, [product_id, rating])
        return ProductReviews(product_id=product_id, reviews=[self._row_to_review(row) for row in rows])

    def get_by_customer(self, customer_id: str) -> CustomerReviews:
        query = "SELECT * FROM reviews_by_customer WHERE customer_id = %s"
        rows = self._session.execute(query, [customer_id])
        return CustomerReviews(customer_id=customer_id, reviews=[self._row_to_review(row) for row in rows])

    def get_top_reviewed_products(self, n: int, period: str) -> List[TopReviewedProduct]:
        query = "SELECT product_id, review_count FROM product_stats_by_period WHERE period = %s LIMIT %s"
        rows = self._session.execute(query, [period, n])
        return [TopReviewedProduct(product_id=row['product_id'], review_count=row['review_count']) for row in rows]

    def get_top_customers_verified(self, n: int, period: str) -> List[TopCustomer]:
        query = "SELECT customer_id, review_count FROM customer_verified_stats_by_period WHERE period = %s LIMIT %s"
        rows = self._session.execute(query, [period, n])
        return [TopCustomer(customer_id=row['customer_id'], review_count=row['review_count']) for row in rows]

    def get_top_haters(self, n: int, period: str) -> List[TopHater]:
        query = "SELECT customer_id, bad_reviews_count FROM hater_stats_by_period WHERE period = %s LIMIT %s"
        rows = self._session.execute(query, [period, n])
        return [TopHater(customer_id=row['customer_id'], bad_reviews_count=row['bad_reviews_count']) for row in rows]

    def get_top_backers(self, n: int, period: str) -> List[TopBacker]:
        query = "SELECT customer_id, good_reviews_count FROM backer_stats_by_period WHERE period = %s LIMIT %s"
        rows = self._session.execute(query, [period, n])
        return [TopBacker(customer_id=row['customer_id'], good_reviews_count=row['good_reviews_count']) for row in rows]

    @staticmethod
    def _row_to_review(row: dict) -> Review:
        # Для словників використовуємо .get(), щоб безпечно отримувати значення
        return Review(
            review_id=row.get('review_id', ''),
            product_id=row.get('product_id', ''),
            customer_id=str(row.get('customer_id', '')),
            star_rating=row.get('star_rating', 0),
            review_date=row.get('review_date', None),
            review_body=row.get('review_body', None)
        )