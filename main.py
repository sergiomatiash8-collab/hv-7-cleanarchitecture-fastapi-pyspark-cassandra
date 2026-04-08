import os
domain_path = os.path.join('src', 'domain')
if os.path.exists(domain_path):
    print(f"Файли в src/domain: {os.listdir(domain_path)}")
else:
    print("Папка src/domain не знайдена!")

import uvicorn
from fastapi import FastAPI
from src.presentation.routes.review_routes import router as review_router

# --- ARCHITECTURE SETUP ---
from src.application.services.review_service import ReviewService

# OLD: In-memory storage (volatile)
# from src.infrastructure.repositories.memory_repository import MemoryReviewRepository
# repo = MemoryReviewRepository()

# NEW: Cassandra storage (persistent)
from src.infrastructure.repositories.cassandra_repository import CassandraReviewRepository
repo = CassandraReviewRepository()

service = ReviewService(repo)
# ---------------------------

app = FastAPI(title="Review Service API")

# Dependency Injection logic usually goes here or in the router
# For now, we make sure our router uses the 'service' we just created
app.state.review_service = service
app.include_router(review_router)

@app.get("/")
def read_root():
    return {"message": "Сервер працює! Перейдіть на /docs для тестування"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)