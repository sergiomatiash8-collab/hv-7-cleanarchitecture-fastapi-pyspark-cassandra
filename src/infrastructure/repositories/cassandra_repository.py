import uuid
from cassandra.cluster import Cluster
from src.domain.entities import Review
from src.domain.interfaces import IReviewRepository

class CassandraReviewRepository(IReviewRepository):
    def __init__(self):
        # Establish connection to the local Cassandra node
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect('review_keyspace')

    def save(self, review: Review):
        # Persistence logic for saving a review
        query = """
        INSERT INTO reviews (review_id, product_id, customer_id, star_rating, review_body, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.session.execute(query, (
            uuid.UUID(review.review_id),
            review.product_id,
            review.customer_id,
            review.star_rating,
            review.review_body,
            review.created_at
        ))
        print(f"[Cassandra] Review {review.review_id} successfully persisted.")

    def get_all(self) -> list[Review]:
        # Fetching all records and mapping them back to Domain Entities
        query = "SELECT * FROM reviews"
        rows = self.session.execute(query)
        
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