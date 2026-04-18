from fastapi import APIRouter, Depends
from src.application.use_cases import (
    GetProductReviewsUseCase,
    GetProductReviewsByRatingUseCase,
    GetCustomerReviewsUseCase,
    GetTopReviewedProductsUseCase,
    GetTopCustomersUseCase,
    GetTopHatersUseCase,
    GetTopBackersUseCase
)
from src.api.dependencies import (
    get_product_reviews_use_case,
    get_product_reviews_by_rating_use_case,
    get_customer_reviews_use_case,
    get_top_products_use_case,
    get_top_customers_use_case,
    get_top_haters_use_case,
    get_top_backers_use_case
)

router = APIRouter(prefix="/reviews", tags=["reviews"])

@router.get("/product/{product_id}")
def get_reviews_by_product(product_id: str, use_case: GetProductReviewsUseCase = Depends(get_product_reviews_use_case)):
    return use_case.execute(product_id)

@router.get("/product/{product_id}/rating/{rating}")
def get_reviews_by_product_and_rating(product_id: str, rating: int, use_case: GetProductReviewsByRatingUseCase = Depends(get_product_reviews_by_rating_use_case)):
    return use_case.execute(product_id, rating)

@router.get("/customer/{customer_id}")
def get_reviews_by_customer(customer_id: str, use_case: GetCustomerReviewsUseCase = Depends(get_customer_reviews_use_case)):
    return use_case.execute(customer_id)

@router.get("/top-products")
def get_top_products(n: int = 10, start: str = "2020-01", end: str = "2020-12", use_case: GetTopReviewedProductsUseCase = Depends(get_top_products_use_case)):
    return use_case.execute(n, start, end)

@router.get("/top-customers")
def get_top_customers(n: int = 10, start: str = "2020-01", end: str = "2020-12", use_case: GetTopCustomersUseCase = Depends(get_top_customers_use_case)):
    return use_case.execute(n, start, end)

@router.get("/top-haters")
def get_top_haters(n: int = 10, start: str = "2020-01", end: str = "2020-12", use_case: GetTopHatersUseCase = Depends(get_top_haters_use_case)):
    return use_case.execute(n, start, end)

@router.get("/top-backers")
def get_top_backers(n: int = 10, start: str = "2020-01", end: str = "2020-12", use_case: GetTopBackersUseCase = Depends(get_top_backers_use_case)):
    return use_case.execute(n, start, end)