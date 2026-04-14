from typing import Optional
# Переводимо на абсолютні імпорти
from src.domain.repositories import ReviewRepository, CacheRepository
from src.domain.entities import ProductReviews, CustomerReviews

class GetProductReviewsUseCase:
    """
    Сценарій використання: Отримання відгуків про продукт.
    
    Реалізує патерн Cache-Aside: 
    1. Перевірка наявності даних у швидкому кеші.
    2. Якщо немає — запит до основної бази та оновлення кешу.
    """
    
    def __init__(
        self, 
        review_repo: ReviewRepository, 
        cache_repo: CacheRepository,
        cache_ttl: int = 60
    ):
        self._review_repo = review_repo
        self._cache_repo = cache_repo
        self._cache_ttl = cache_ttl
    
    def execute(self, product_id: str) -> dict:
        cache_key = f"product:{product_id}"
        
        cached = self._cache_repo.get(cache_key)
        if cached:
            return cached
        
        product_reviews = self._review_repo.get_by_product(product_id)
        response = self._format_response(product_reviews)
        
        self._cache_repo.set(cache_key, response, self._cache_ttl)
        return response
    
    @staticmethod
    def _format_response(product_reviews: ProductReviews) -> dict:
        return {
            "product_id": product_reviews.product_id,
            "count": product_reviews.count,
            "reviews": [
                {
                    "review_id": r.review_id,
                    "star_rating": r.star_rating,
                    "review_date": str(r.review_date),
                    "review_body": r.review_body
                }
                for r in product_reviews.reviews
            ]
        }

class GetProductReviewsByRatingUseCase:
    """
    Сценарій використання: Фільтрація відгуків за рейтингом.
    """
    
    def __init__(
        self, 
        review_repo: ReviewRepository, 
        cache_repo: CacheRepository,
        cache_ttl: int = 60
    ):
        self._review_repo = review_repo
        self._cache_repo = cache_repo
        self._cache_ttl = cache_ttl
    
    def execute(self, product_id: str, rating: int) -> dict:
        cache_key = f"product:{product_id}:rating:{rating}"
        
        cached = self._cache_repo.get(cache_key)
        if cached:
            return cached
        
        product_reviews = self._review_repo.get_by_product_and_rating(product_id, rating)
        
        response = {
            "product_id": product_id,
            "rating": rating,
            "count": product_reviews.count,
            "reviews": [
                {
                    "review_id": r.review_id,
                    "star_rating": r.star_rating,
                    "review_date": str(r.review_date)
                }
                for r in product_reviews.reviews
            ]
        }
        
        self._cache_repo.set(cache_key, response, self._cache_ttl)
        return response

class GetCustomerReviewsUseCase:
    """
    Сценарій використання: Перегляд активності клієнта.
    """
    
    def __init__(
        self, 
        review_repo: ReviewRepository, 
        cache_repo: CacheRepository,
        cache_ttl: int = 60
    ):
        self._review_repo = review_repo
        self._cache_repo = cache_repo
        self._cache_ttl = cache_ttl
    
    def execute(self, customer_id: str) -> dict:
        cache_key = f"customer:{customer_id}"
        
        cached = self._cache_repo.get(cache_key)
        if cached:
            return cached
        
        customer_reviews = self._review_repo.get_by_customer(customer_id)
        
        response = {
            "customer_id": customer_id,
            "count": customer_reviews.count,
            "reviews": [
                {
                    "review_id": r.review_id,
                    "product_id": r.product_id,
                    "star_rating": r.star_rating
                }
                for r in customer_reviews.reviews
            ]
        }
        
        self._cache_repo.set(cache_key, response, self._cache_ttl)
        return response