import redis
from cassandra.cluster import Cluster
from cassandra.io.geventreactor import GeventConnection
from src.config.settings import settings
from src.infrastructure.cassandra_repository import CassandraReviewRepository
from src.infrastructure.redis_cache import RedisCache
from src.application.use_cases import (
    GetProductReviewsUseCase,
    GetProductReviewsByRatingUseCase,
    GetCustomerReviewsUseCase,
    GetTopReviewedProductsUseCase,
    GetTopCustomersUseCase,
    GetTopHatersUseCase,
    GetTopBackersUseCase
)

_cassandra_session = None
_redis_client = None

def init_dependencies():
    global _cassandra_session, _redis_client
    cluster = Cluster([settings.cassandra_host], port=settings.cassandra_port)
    cluster.connection_class = GeventConnection
    _cassandra_session = cluster.connect(settings.cassandra_keyspace)
    _redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True
    )

def _get_repos():
    return CassandraReviewRepository(_cassandra_session), RedisCache(_redis_client)

def get_product_reviews_use_case():
    repo, cache = _get_repos()
    return GetProductReviewsUseCase(repo, cache, 300)

def get_product_reviews_by_rating_use_case():
    repo, cache = _get_repos()
    return GetProductReviewsByRatingUseCase(repo, cache, 300)

def get_customer_reviews_use_case():
    repo, cache = _get_repos()
    return GetCustomerReviewsUseCase(repo, cache, 300)

def get_top_products_use_case():
    repo, cache = _get_repos()
    return GetTopReviewedProductsUseCase(repo, cache, 300)

def get_top_customers_use_case():
    repo, cache = _get_repos()
    return GetTopCustomersUseCase(repo, cache, 300)

def get_top_haters_use_case():
    repo, cache = _get_repos()
    return GetTopHatersUseCase(repo, cache, 300)

def get_top_backers_use_case():
    repo, cache = _get_repos()
    return GetTopBackersUseCase(repo, cache, 300)