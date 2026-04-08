from fastapi import APIRouter, Depends
from typing import List
from src.application.services.review_service import ReviewService
from src.infrastructure.repositories.cassandra_repository import CassandraReviewRepository
from pydantic import BaseModel

router = APIRouter()

# Schema for incoming data (POST)
class ReviewRequest(BaseModel):
    product_id: str
    customer_id: str
    star_rating: int
    review_body: str

# Dependency injection for the service
def get_service():
    repo = CassandraReviewRepository()
    return ReviewService(repo)

@router.post("/reviews")
def create_review(request_data: ReviewRequest, service: ReviewService = Depends(get_service)):
    review = service.create_review(
        product_id=request_data.product_id,
        customer_id=request_data.customer_id,
        rating=request_data.star_rating,
        body=request_data.review_body
    )
    return {"status": "success", "review_id": review.review_id}

@router.get("/reviews")
def get_reviews(service: ReviewService = Depends(get_service)):
    # Fetching all reviews from the service layer
    reviews = service.get_all_reviews()
    return reviews