import uuid
from cassandra.cluster import Cluster
from src.domain.entities import Review
from src.domain.interfaces import IReviewRepository

class CassandraReviewRepository(IReviewRepository):
    def __init__(self):
        # Connecting to the Cassandra container by its service name in Docker network
        # Using the correct keyspace name: 'reviews_keyspace'
        self.cluster = Cluster(['cassandra_dev'])
        self.session = self.cluster.connect('reviews_keyspace')

    def save(self, review: Review):
        """
        Persists a single review entity into the 'reviews' table.
        """
        query = """
        INSERT INTO reviews (review_id, product_id, customer_id, star_rating, review_body, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        # Ensure review_id is a valid UUID object for Cassandra
        review_uuid = uuid.UUID(review.review_id) if isinstance(review.review_id, str) else review.review_id
        
        self.session.execute(query, (
            review_uuid,
            review.product_id,
            review.customer_id,
            review.star_rating,
            review.review_body,
            review.created_at
        ))
        print(f"[Cassandra] Review {review.review_id} successfully persisted.")

    def get_all(self) -> list[Review]:
        """
        Fetches all records from the 'reviews' table and maps them to Domain Entities.
        """
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

    def close(self):
        """
        Properly closes the cluster connection.
        """
        self.cluster.shutdown()