import uuid
from cassandra.cluster import Cluster
from src.domain.entities import Review
from src.domain.interfaces import IReviewRepository

class CassandraReviewRepository(IReviewRepository):
    def __init__(self):
        # Connecting to the Cassandra cluster
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect('review_keyspace')

    def save(self, review: Review):
        # Infrastructure layer: mapping domain entity to CQL query
        query = """
        INSERT INTO reviews (review_id, product_id, customer_id, star_rating, review_body, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        # We convert review_id string back to a real UUID object for Cassandra
        self.session.execute(query, (
            uuid.UUID(review.review_id), 
            review.product_id,
            review.customer_id,
            review.star_rating,
            review.review_body,
            review.created_at
        ))
        print(f"[Cassandra] Відгук {review.review_id} збережено в базу!")