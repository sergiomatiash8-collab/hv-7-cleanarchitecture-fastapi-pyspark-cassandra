from fastapi import FastAPI, HTTPException
from src.infrastructure.cassandra_repository import CassandraReviewRepository


# Handles an HTTP request by retrieving data from the repository
# and returning it as a JSON response.

app = FastAPI(title="Cassandra Reviews API")

repo = CassandraReviewRepository()

@app.get("/")
async def root():
    return {"message": "Cassandra ETL API is running"}

@app.get("/reviews")
async def get_all_reviews():
    """
    Endpoint to fetch all reviews from Cassandra.
    """
    try:
        reviews = repo.get_all()
        return reviews
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("shutdown")
def shutdown_event():
    repo.close()