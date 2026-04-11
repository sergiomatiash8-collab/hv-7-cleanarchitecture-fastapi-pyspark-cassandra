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
async def get_top_haters(
    request: Request,
    period: str = None,
    limit: int = 10
):
    repo = request.app.state.analytics_repo
    redis = request.app.state.redis

    cache_key = f"analytics:top_haters:{period}:{limit}"

    try:
        cached_data = redis.get_cache(cache_key)
        if cached_data:
            return cached_data

        data = repo.get_top_haters(period, limit)

        redis.set_cache(cache_key, data, ttl=600)
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-products", response_model=List[ProductMetric])
async def get_top_products(
    request: Request,
    period: str = None,
    limit: int = 10
):
    repo = request.app.state.analytics_repo
    redis = request.app.state.redis

    cache_key = f"analytics:top_products:{period}:{limit}"

    try:
        cached_data = redis.get_cache(cache_key)
        if cached_data:
            return cached_data

        data = repo.get_top_products(period, limit)

        redis.set_cache(cache_key, data, ttl=600)
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))