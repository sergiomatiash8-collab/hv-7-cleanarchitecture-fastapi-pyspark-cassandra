import uvicorn
from fastapi import FastAPI
from src.presentation.routes.review_routes import router as review_router
from src.api.routers.analytics import router as analytics_router 

from src.application.services.review_service import ReviewService
from src.infrastructure.repositories.cassandra_repository import CassandraReviewRepository
from src.infrastructure.repositories.analytics_repository import AnalyticsRepository
from src.infrastructure.redis.client import RedisClient

# --- ARCHITECTURE SETUP ---

# 1. Інфраструктура
repo = CassandraReviewRepository()
analytics_repo = AnalyticsRepository()
redis_cache = RedisClient()

# 2. Сервіс
service = ReviewService(repo, redis_cache)

# ---------------------------

app = FastAPI(title="Review Service API")

# 3. Реєструємо сервіси та роутери в app.state
app.state.review_service = service
app.state.analytics_repo = analytics_repo
app.state.redis = redis_cache  # ДОДАНО: реєструємо Redis для аналітики

app.include_router(review_router)
app.include_router(analytics_router) 

@app.get("/")
def read_root():
    return {"status": "online", "message": "Server is running!"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)