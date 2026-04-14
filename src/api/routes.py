from fastapi import APIRouter, Depends
from src.application.use_cases import (
    GetProductReviewsUseCase,
    GetProductReviewsByRatingUseCase,
    GetCustomerReviewsUseCase
)
from src.api.dependencies import (
    get_product_reviews_use_case,
    get_product_reviews_by_rating_use_case,
    get_customer_reviews_use_case
)

# Ініціалізація роутера. 
# prefix дозволяє групувати ендпоінти, а tags автоматично структурує документацію Swagger/OpenAPI.
router = APIRouter(prefix="/reviews", tags=["reviews"])

@router.get("/product/{product_id}")
def get_reviews_by_product(
    product_id: str,
    # Використання FastAPI Depends реалізує паттерн Inversion of Control (IoC).
    # Контролер не знає, як створювати UseCase, він отримує його вже "зібраним" через ін'єкцію.
    use_case: GetProductReviewsUseCase = Depends(get_product_reviews_use_case)
):
    """
    Ендпоінт для отримання повного списку відгуків про конкретний товар.
    """
    return use_case.execute(product_id)

@router.get("/product/{product_id}/rating/{rating}")
def get_reviews_by_product_and_rating(
    product_id: str,
    rating: int,
    use_case: GetProductReviewsByRatingUseCase = Depends(get_product_reviews_by_rating_use_case)
):
    """
    Фільтрований запит відгуків за ідентифікатором товару та зірковим рейтингом.
    """
    return use_case.execute(product_id, rating)

@router.get("/customer/{customer_id}")
def get_reviews_by_customer(
    customer_id: str,
    use_case: GetCustomerReviewsUseCase = Depends(get_customer_reviews_use_case)
):
    """
    Ендпоінт для перегляду профілю відгуків клієнта.
    """
    return use_case.execute(customer_id)