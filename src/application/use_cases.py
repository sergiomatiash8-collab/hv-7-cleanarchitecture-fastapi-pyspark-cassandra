from typing import List
from src.domain.repositories import ReviewRepository, CacheRepository
from src.domain.entities import (
    ProductReviews, CustomerReviews, 
    TopReviewedProduct, TopCustomer, TopHater, TopBacker
)

class GetProductReviewsUseCase:
    def __init__(self, review_repo: ReviewRepository, cache_repo: CacheRepository, cache_ttl: int = 300):
        self._review_repo = review_repo
        self._cache_repo = cache_repo
        self._cache_ttl = cache_ttl
    
    def execute(self, product_id: str) -> dict:
        cache_key = f"product:{product_id}"
        cached = self._cache_repo.get(cache_key)
        if cached: return cached
        
        data = self._review_repo.get_by_product(product_id)
        res = {
            "product_id": data.product_id,
            "count": data.count,
            "reviews": [{"id": r.review_id, "rating": r.star_rating, "date": str(r.review_date), "body": r.review_body} for r in data.reviews]
        }
        self._cache_repo.set(cache_key, res, self._cache_ttl)
        return res

class GetProductReviewsByRatingUseCase:
    def __init__(self, review_repo: ReviewRepository, cache_repo: CacheRepository, cache_ttl: int = 300):
        self._review_repo = review_repo
        self._cache_repo = cache_repo
        self._cache_ttl = cache_ttl
    
    def execute(self, product_id: str, rating: int) -> dict:
        cache_key = f"product:{product_id}:rating:{rating}"
        cached = self._cache_repo.get(cache_key)
        if cached: return cached
        
        data = self._review_repo.get_by_product_and_rating(product_id, rating)
        res = {
            "product_id": product_id,
            "rating": rating,
            "count": data.count,
            "reviews": [{"id": r.review_id, "rating": r.star_rating, "date": str(r.review_date)} for r in data.reviews]
        }
        self._cache_repo.set(cache_key, res, self._cache_ttl)
        return res

class GetCustomerReviewsUseCase:
    def __init__(self, review_repo: ReviewRepository, cache_repo: CacheRepository, cache_ttl: int = 300):
        self._review_repo = review_repo
        self._cache_repo = cache_repo
        self._cache_ttl = cache_ttl
    
    def execute(self, customer_id: str) -> dict:
        cache_key = f"customer:{customer_id}"
        cached = self._cache_repo.get(cache_key)
        if cached: return cached
        
        data = self._review_repo.get_by_customer(customer_id)
        res = {
            "customer_id": customer_id,
            "count": data.count,
            "reviews": [{"id": r.review_id, "product": r.product_id, "rating": r.star_rating} for r in data.reviews]
        }
        self._cache_repo.set(cache_key, res, self._cache_ttl)
        return res

class GetTopReviewedProductsUseCase:
    def __init__(self, repo: ReviewRepository, cache: CacheRepository, ttl: int = 300):
        self.repo, self.cache, self.ttl = repo, cache, ttl

    def execute(self, n: int, start: str, end: str) -> List[dict]:
        key = f"top_prod:{n}:{start}:{end}"
        cached = self.cache.get(key)
        if cached: return cached
        res = [r.__dict__ for r in self.repo.get_top_reviewed_products(n, start, end)]
        self.cache.set(key, res, self.ttl)
        return res

class GetTopCustomersUseCase:
    def __init__(self, repo: ReviewRepository, cache: CacheRepository, ttl: int = 300):
        self.repo, self.cache, self.ttl = repo, cache, ttl

    def execute(self, n: int, start: str, end: str) -> List[dict]:
        key = f"top_cust:{n}:{start}:{end}"
        cached = self.cache.get(key)
        if cached: return cached
        res = [r.__dict__ for r in self.repo.get_top_customers_verified(n, start, end)]
        self.cache.set(key, res, self.ttl)
        return res

class GetTopHatersUseCase:
    def __init__(self, repo: ReviewRepository, cache: CacheRepository, ttl: int = 300):
        self.repo, self.cache, self.ttl = repo, cache, ttl

    def execute(self, n: int, start: str, end: str) -> List[dict]:
        key = f"top_haters:{n}:{start}:{end}"
        cached = self.cache.get(key)
        if cached: return cached
        res = [r.__dict__ for r in self.repo.get_top_haters(n, start, end)]
        self.cache.set(key, res, self.ttl)
        return res

class GetTopBackersUseCase:
    def __init__(self, repo: ReviewRepository, cache: CacheRepository, ttl: int = 300):
        self.repo, self.cache, self.ttl = repo, cache, ttl

    def execute(self, n: int, start: str, end: str) -> List[dict]:
        key = f"top_backers:{n}:{start}:{end}"
        cached = self.cache.get(key)
        if cached: return cached
        res = [r.__dict__ for r in self.repo.get_top_backers(n, start, end)]
        self.cache.set(key, res, self.ttl)
        return res