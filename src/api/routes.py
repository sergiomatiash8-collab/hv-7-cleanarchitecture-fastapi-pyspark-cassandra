from fastapi import APIRouter, Depends
from ..application.use_cases import (
    GetProductReviewsUseCase,
    GetProductReviewsByRatingUseCase,
    GetCustomerReviewsUseCase
)
from .dependencies import (
    get_product_reviews_use_case,
    get_product_reviews_by_rating_use_case,
    get_customer_reviews_use_case
)

router = APIRouter(prefix="/reviews", tags=["reviews"])

@router.get("/product/{product_id}")
def get_reviews_by_product(
    product_id: str,
    use_case: GetProductReviewsUseCase = Depends(get_product_reviews_use_case)
):
    """Отримати всі відгуки для продукту"""
    return use_case.execute(product_id)

@router.get("/product/{product_id}/rating/{rating}")
def get_reviews_by_product_and_rating(
    product_id: str,
    rating: int,
    use_case: GetProductReviewsByRatingUseCase = Depends(get_product_reviews_by_rating_use_case)
):
    """Отримати відгуки продукту з певним рейтингом"""
    return use_case.execute(product_id, rating)

@router.get("/customer/{customer_id}")
def get_reviews_by_customer(
    customer_id: str,
    use_case: GetCustomerReviewsUseCase = Depends(get_customer_reviews_use_case)
):
    """Отримати всі відгуки клієнта"""
    return use_case.execute(customer_id)