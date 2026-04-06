from src.domain.entities import Review
from src.domain.interfaces import ReviewRepository
from datetime import datetime
import uuid

#use cases logic

class ReviewService:
    def __init__(self, repository: ReviewRepository):
        
        self.repository = repository

    def create_review(self, product_id: str, customer_id: str, rating: int, body: str) -> Review:
        
        new_review = Review(
            review_id=str(uuid.uuid4()),  
            product_id=product_id,
            customer_id=customer_id,
            star_rating=rating,
            review_body=body,
            review_date=datetime.now()
        )
        
       
        self.repository.save(new_review)
        
        return new_review