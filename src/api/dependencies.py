from functools import lru_cache
from cassandra.cluster import Cluster
from cassandra.io.geventreactor import GeventConnection
import redis
# Абсолютні імпорти
from src.config.settings import settings
from src.infrastructure.cassandra_repository import CassandraReviewRepository
from src.infrastructure.redis_cache import RedisCache
from src.application.use_cases import (
    GetProductReviewsUseCase,
    GetProductReviewsByRatingUseCase,
    GetCustomerReviewsUseCase
)

# Глобальні змінні для з'єднань (Connection Pooling)
_cassandra_session = None
_redis_client = None

def init_dependencies():
    """
    Головний ініціалізатор інфраструктурного рівня.
    """
    global _cassandra_session, _redis_client
    
    # Налаштування кластера Cassandra
    cluster = Cluster([settings.cassandra_host], port=settings.cassandra_port)
    cluster.connection_class = GeventConnection
    _cassandra_session = cluster.connect(settings.cassandra_keyspace)
    
    # Налаштування клієнта Redis
    _redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True
    )
    
    print("✅ Залежності ініціалізовано")

def get_product_reviews_use_case() -> GetProductReviewsUseCase:
    """
    Фабричний метод для створення Use Case "Отримання відгуків про продукт".
    """
    review_repo = CassandraReviewRepository(_cassandra_session)
    cache_repo = RedisCache(_redis_client)
    
    # Використовуємо settings.cache_ttl (у тебе в ETLSettings він є, 
    # але переконайся, що він доступний і для API)
    return GetProductReviewsUseCase(review_repo, cache_repo, getattr(settings, 'cache_ttl', 60))

def get_product_reviews_by_rating_use_case() -> GetProductReviewsByRatingUseCase:
    """
    Фабричний метод для Use Case фільтрації за рейтингом.
    """
    review_repo = CassandraReviewRepository(_cassandra_session)
    cache_repo = RedisCache(_redis_client)
    return GetProductReviewsByRatingUseCase(review_repo, cache_repo, getattr(settings, 'cache_ttl', 60))

def get_customer_reviews_use_case() -> GetCustomerReviewsUseCase:
    """
    Фабричний метод для Use Case отримання відгуків конкретного клієнта.
    """
    review_repo = CassandraReviewRepository(_cassandra_session)
    cache_repo = RedisCache(_redis_client)
    return GetCustomerReviewsUseCase(review_repo, cache_repo, getattr(settings, 'cache_ttl', 60))