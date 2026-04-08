import uuid
from datetime import datetime
from typing import List
from src.domain.entities import Review
from src.domain.interfaces import IReviewRepository

class ReviewService:
    def __init__(self, repository: IReviewRepository):
        self.repository = repository

    def create_review(self, product_id: str, customer_id: str, rating: int, body: str) -> Review:
        new_review = Review(
            review_id=str(uuid.uuid4()),
            product_id=product_id,
            customer_id=customer_id,
            star_rating=rating,
            review_body=body,
            created_at=datetime.now()
        )
        self.repository.save(new_review)
        return new_review

    def get_all_reviews(self) -> List[Review]:
        # Simply delegate the call to the repository
        return self.repository.get_all()