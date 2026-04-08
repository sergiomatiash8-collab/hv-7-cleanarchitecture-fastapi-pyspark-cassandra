import uuid
from datetime import datetime
from src.domain.entities import Review
from src.domain.interfaces import IReviewRepository

class ReviewService:
    def __init__(self, repository: IReviewRepository):
        self.repository = repository

    def create_review(self, product_id: str, customer_id: str, rating: int, body: str) -> Review:
        # Business logic: creating a new review with unified naming
        new_review = Review(
            review_id=str(uuid.uuid4()),
            product_id=product_id,
            customer_id=customer_id,
            star_rating=rating,
            review_body=body,
            created_at=datetime.now()  # Changed from review_date to created_at
        )
        
        self.repository.save(new_review)
        return new_review