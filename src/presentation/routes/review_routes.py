from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from src.application.services.review_service import ReviewService
from src.infrastructure.repositories.memory_repository import MemoryReviewRepository

#communication

router = APIRouter()


repo = MemoryReviewRepository()
service = ReviewService(repo)


class ReviewCreateRequest(BaseModel):
    product_id: str
    customer_id: str
    star_rating: int
    review_body: str


@router.post("/reviews")
def create_review(request: ReviewCreateRequest):
    
    review = service.create_review(
        product_id=request.product_id,
        customer_id=request.customer_id,
        rating=request.star_rating,
        body=request.review_body
    )
    return {"status": "success", "review_id": review.review_id}