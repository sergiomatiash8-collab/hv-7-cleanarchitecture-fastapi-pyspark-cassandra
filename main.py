import uvicorn
from fastapi import FastAPI
from src.presentation.routes.review_routes import router as review_router
from src.application.services.review_service import ReviewService
from src.infrastructure.repositories.cassandra_repository import CassandraReviewRepository
from src.infrastructure.redis.client import RedisClient # Додаємо клієнт

# --- ARCHITECTURE SETUP ---

# 1. Інфраструктура
repo = CassandraReviewRepository()
redis_cache = RedisClient() # Ініціалізуємо Redis

# 2. Сервіс (тепер приймає і репо, і кеш)
service = ReviewService(repo, redis_cache)

# ---------------------------

app = FastAPI(title="Review Service API")
app.state.review_service = service
app.include_router(review_router)

@app.get("/")
def read_root():
    return {"status": "online", "message": "Server is running!"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)