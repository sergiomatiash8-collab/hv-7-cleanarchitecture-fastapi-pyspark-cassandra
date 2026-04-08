from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

# communication
router = APIRouter()

class ReviewCreateRequest(BaseModel):
    product_id: str
    customer_id: str
    star_rating: int
    review_body: str

@router.post("/reviews")
def create_review(request_data: ReviewCreateRequest, request: Request):
    # Get service from app state to ensure persistent storage usage
    service = request.app.state.review_service
    
    review = service.create_review(
        product_id=request_data.product_id,
        customer_id=request_data.customer_id,
        rating=request_data.star_rating,
        body=request_data.review_body
    )
    
    return {"status": "success", "review_id": review.review_id}