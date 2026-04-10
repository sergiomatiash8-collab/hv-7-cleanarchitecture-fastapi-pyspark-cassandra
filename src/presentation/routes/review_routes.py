from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from datetime import datetime

router = APIRouter()

# Schema for incoming data (POST)
class ReviewRequest(BaseModel):
    product_id: str
    customer_id: str
    star_rating: int
    review_body: str

@router.post("/reviews")
def create_review(request_data: ReviewRequest, request: Request):
    # Get the service from app.state (initialized in main.py)
    service = request.app.state.review_service
    
    try:
        review = service.create_review(
            product_id=request_data.product_id,
            customer_id=request_data.customer_id,
            rating=request_data.star_rating,
            body=request_data.review_body
        )
        return {"status": "success", "review_id": review.review_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/reviews")
def get_reviews(request: Request):
    """
    Fetches all reviews from Cassandra using the shared service instance.
    """
    service = request.app.state.review_service
    
    try:
        reviews = service.get_all_reviews()
        return reviews
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))