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
from src.infrastructure.cassandra.database import CassandraConnection

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_cassandra():
    """Ініціалізація Cassandra: з'єднання + міграції"""
    try:
        logger.info("🔌 Connecting to Cassandra...")
        from cassandra.cluster import Cluster
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        
        # Запускаємо міграції
        logger.info("🚀 Running Cassandra migrations...")
        migrations = CassandraMigrations(session)
        migrations.run_all_migrations()
        
        return cluster, session
    except Exception as e:
        logger.error(f"❌ Cassandra initialization failed: {e}")
        raise

# --- ARCHITECTURE SETUP ---

try:
    # Инфраструктура
    cassandra_cluster, cassandra_session = init_cassandra()
    repo = CassandraReviewRepository()
    analytics_repo = AnalyticsRepository()
    redis_cache = RedisClient()
    
    # Сервіс (з моками для Redis на випадок, якщо не передається)
    service = ReviewService(repo, redis_cache)
    
    logger.info("✅ All components initialized successfully")
except Exception as e:
    logger.error(f"❌ Failed to initialize components: {e}")
    raise

# --- FASTAPI APP ---

app = FastAPI(
    title="HV-7 Review Service API",
    description="Clean Architecture: Reviews + Analytics + Cassandra + Redis",
    version="1.0.0"
)

# Реєструємо сервіси в app.state для використання в endpoints
app.state.review_service = service
app.state.analytics_repo = analytics_repo
app.state.redis = redis_cache
app.state.cassandra_repo = repo

# Роутери
app.include_router(review_router)
app.include_router(analytics_router)

@app.get("/")
def read_root():
    return {
        "status": "online",
        "message": "HV-7 Review Analytics API",
        "version": "1.0.0",
        "docs_url": "/docs"
    }

@app.get("/health")
def health_check():
    """Простий health check"""
    return {"status": "ok", "service": "review-api"}

if __name__ == "__main__":
    try:
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
        repo.close()
        analytics_repo.close()
        cassandra_cluster.shutdown()
    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        raise