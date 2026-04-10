import os
import uvicorn
from fastapi import FastAPI
from src.presentation.routes.review_routes import router as review_router
from src.application.services.review_service import ReviewService
from src.infrastructure.repositories.cassandra_repository import CassandraReviewRepository

# Check domain files for debugging
domain_path = os.path.join('src', 'domain')
if os.path.exists(domain_path):
    print(f"Files in src/domain: {os.listdir(domain_path)}")
else:
    print("Directory src/domain not found!")

# --- ARCHITECTURE SETUP ---

# Initialize Cassandra storage (persistent)
# Ensure CassandraReviewRepository uses host='cassandra_dev' inside
repo = CassandraReviewRepository()

# Initialize Service with Repository
service = ReviewService(repo)

# ---------------------------

app = FastAPI(title="Review Service API")

# Store the service in app.state so routers can access it
app.state.review_service = service

# Include our routes
app.include_router(review_router)

@app.get("/")
def read_root():
    return {
        "status": "online",
        "message": "Server is running! Go to /docs for API testing"
    }

if __name__ == "__main__":
    # CRITICAL: host must be 0.0.0.0 for Docker port mapping to work
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)