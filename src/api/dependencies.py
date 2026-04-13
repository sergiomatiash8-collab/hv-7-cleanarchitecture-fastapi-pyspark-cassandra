from functools import lru_cache
from cassandra.cluster import Cluster
from cassandra.io.geventreactor import GeventConnection
import redis
from ..config.settings import settings
from ..infrastructure.cassandra_repository import CassandraReviewRepository
from ..infrastructure.redis_cache import RedisCache
from ..application.use_cases import (
    GetProductReviewsUseCase,
    GetProductReviewsByRatingUseCase,
    GetCustomerReviewsUseCase
)

# Глобальні змінні для з'єднань (ініціалізуються при старті)
_cassandra_session = None
_redis_client = None

def init_dependencies():
    """Ініціалізація залежностей при старті додатку"""
    global _cassandra_session, _redis_client
    
    # Cassandra
    cluster = Cluster([settings.cassandra_host], port=settings.cassandra_port)
    cluster.connection_class = GeventConnection
    _cassandra_session = cluster.connect(settings.cassandra_keyspace)
    
    # Redis
    _redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True
    )
    
    print("✅ Залежності ініціалізовано")

def get_product_reviews_use_case() -> GetProductReviewsUseCase:
    """Dependency injection для use case"""
    review_repo = CassandraReviewRepository(_cassandra_session)
    cache_repo = RedisCache(_redis_client)
    return GetProductReviewsUseCase(review_repo, cache_repo, settings.cache_ttl)

def get_product_reviews_by_rating_use_case() -> GetProductReviewsByRatingUseCase:
    review_repo = CassandraReviewRepository(_cassandra_session)
    cache_repo = RedisCache(_redis_client)
    return GetProductReviewsByRatingUseCase(review_repo, cache_repo, settings.cache_ttl)

def get_customer_reviews_use_case() -> GetCustomerReviewsUseCase:
    review_repo = CassandraReviewRepository(_cassandra_session)
    cache_repo = RedisCache(_redis_client)
    return GetCustomerReviewsUseCase(review_repo, cache_repo, settings.cache_ttl)