import uvicorn
import logging
from fastapi import FastAPI

from src.presentation.routes.review_routes import router as review_router
from src.api.routers.analytics import router as analytics_router

from src.application.services.review_service import ReviewService
from src.infrastructure.repositories.cassandra_repository import CassandraReviewRepository
from src.infrastructure.repositories.analytics_repository import AnalyticsRepository
from src.infrastructure.redis.client import RedisClient
from src.infrastructure.cassandra.migrations import CassandraMigrations
from src.infrastructure.cassandra.database import CassandraSession


# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# ---------------- CASSANDRA INIT ----------------
def init_cassandra():
    try:
        logger.info("🔌 Connecting to Cassandra...")

        cassandra = CassandraSession(hosts=["hv7-cassandra-dev"])
        session = cassandra.connect()

        logger.info("🚀 Running Cassandra migrations...")
        migrations = CassandraMigrations(session)
        migrations.run_all_migrations()

        return cassandra, session

    except Exception as e:
        logger.error(f"❌ Cassandra initialization failed: {e}")
        raise


# ---------------- APP SETUP ----------------
try:
    cassandra_client, cassandra_session = init_cassandra()

    repo = CassandraReviewRepository()
    analytics_repo = AnalyticsRepository()
    redis_cache = RedisClient()

    service = ReviewService(repo, redis_cache)

    logger.info("✅ All components initialized successfully")

except Exception as e:
    logger.error(f"❌ Failed to initialize components: {e}")
    raise


# ---------------- FASTAPI ----------------
app = FastAPI(
    title="HV-7 Review Service API",
    description="Clean Architecture: Reviews + Analytics + Cassandra + Redis",
    version="1.0.0"
)


app.state.review_service = service
app.state.analytics_repo = analytics_repo
app.state.redis = redis_cache
app.state.cassandra_repo = repo


app.include_router(review_router)
app.include_router(analytics_router)


@app.get("/")
def root():
    return {
        "status": "online",
        "message": "HV-7 Review Analytics API",
        "docs": "/docs"
    }


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------- MAIN ----------------
if __name__ == "__main__":
    try:
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True
        )
    except KeyboardInterrupt:
        logger.info("🛑 Shutdown")

        repo.close()
        analytics_repo.close()
        cassandra_client.close()

    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        raise