from cassandra.cluster import Session
from typing import List, Optional
from datetime import date
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

    def get_top_reviewed_products(self, n: int, start_date: str, end_date: str) -> List[TopReviewedProduct]:
        query = "SELECT product_id, review_count FROM product_stats_by_period WHERE period >= %s AND period <= %s LIMIT %s"
        rows = self._session.execute(query, [start_date, end_date, n])
        return [TopReviewedProduct(row.product_id, row.review_count) for row in rows]

    def get_top_customers_verified(self, n: int, start_date: str, end_date: str) -> List[TopCustomer]:
        query = "SELECT customer_id, review_count FROM customer_verified_stats_by_period WHERE period >= %s AND period <= %s LIMIT %s"
        rows = self._session.execute(query, [start_date, end_date, n])
        return [TopCustomer(row.customer_id, row.review_count) for row in rows]

    def get_top_haters(self, n: int, start_date: str, end_date: str) -> List[TopHater]:
        query = "SELECT customer_id, bad_reviews_count FROM hater_stats_by_period WHERE period >= %s AND period <= %s LIMIT %s"
        rows = self._session.execute(query, [start_date, end_date, n])
        return [TopHater(row.customer_id, row.bad_reviews_count) for row in rows]

    def get_top_backers(self, n: int, start_date: str, end_date: str) -> List[TopBacker]:
        query = "SELECT customer_id, good_reviews_count FROM backer_stats_by_period WHERE period >= %s AND period <= %s LIMIT %s"
        rows = self._session.execute(query, [start_date, end_date, n])
        return [TopBacker(row.customer_id, row.good_reviews_count) for row in rows]

    @staticmethod
    def _row_to_review(row) -> Review:
        return Review(
            review_id=row.review_id,
            product_id=row.product_id,
            customer_id=getattr(row, 'customer_id', ''),
            star_rating=row.star_rating,
            review_date=row.review_date,
            review_body=getattr(row, 'review_body', None)
        )