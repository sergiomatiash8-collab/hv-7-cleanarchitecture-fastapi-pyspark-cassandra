from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime

router = APIRouter(prefix="/reviews", tags=["Reviews"])

class ReviewRequest(BaseModel):
    product_id: str
    customer_id: str
    star_rating: int
    review_body: str

@router.get("/product/{product_id}")
def get_by_product(product_id: str, request: Request):
    """Отримати всі відгуки на товар (з кешуванням 5 хв)"""
    service = request.app.state.review_service
    reviews = service.get_reviews_by_product(product_id)
    if not reviews:
        raise HTTPException(status_code=404, detail="No reviews found for this product")
    return reviews

@router.get("/customer/{customer_id}")
def get_by_customer(customer_id: str, request: Request):
    """Отримати всі відгуки покупця (з кешуванням 5 хв)"""
    service = request.app.state.review_service
    reviews = service.get_reviews_by_customer(customer_id)
    if not reviews:
        raise HTTPException(status_code=404, detail="No reviews found for this customer")
    return reviews

@router.post("/")
def create_review(request_data: ReviewRequest, request: Request):
    """Створення відгуку з автоматичним записом у всі таблиці"""
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