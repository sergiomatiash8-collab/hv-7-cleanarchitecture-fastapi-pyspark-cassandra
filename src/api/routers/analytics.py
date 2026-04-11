from fastapi import APIRouter, HTTPException, Request
from typing import List
from pydantic import BaseModel

router = APIRouter(prefix="/analytics", tags=["analytics"])

class UserMetric(BaseModel):
    customer_id: str
    total_reviews: int

class ProductMetric(BaseModel):
    product_id: str
    review_count: int
    avg_rating: float

@router.get("/haters", response_model=List[UserMetric])
async def get_top_haters(request: Request):
    """Повертає топ користувачів, які ставлять 1 зірку (з кешуванням)"""
    repo = request.app.state.analytics_repo
    redis = request.app.state.redis
    cache_key = "analytics:top_haters"

    try:
        # 1. Використовуємо твій метод get_cache
        cached_data = redis.get_cache(cache_key)
        if cached_data:
            return cached_data

        # 2. Якщо в кеші немає — йдемо в базу
        data = repo.get_top_haters()

        # 3. Використовуємо твій метод set_cache (на 10 хв)
        redis.set_cache(cache_key, data, ttl=600)
        
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/top-products", response_model=List[ProductMetric])
async def get_top_products(request: Request):
    """Повертає топ товарів за кількістю відгуків (з кешуванням)"""
    repo = request.app.state.analytics_repo
    redis = request.app.state.redis
    cache_key = "analytics:top_products"

    try:
        cached_data = redis.get_cache(cache_key)
        if cached_data:
            return cached_data

        data = repo.get_top_products()
        redis.set_cache(cache_key, data, ttl=600)
        
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))