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

# Глобальні змінні для з'єднань (Connection Pooling)
# Згідно з Clean Code, ми тримаємо ці об'єкти живими протягом усього життєвого циклу 
# додатка, щоб уникнути витрат на постійне відкриття/закриття TCP-з'єднань.
_cassandra_session = None
_redis_client = None

def init_dependencies():
    """
    Головний ініціалізатор інфраструктурного рівня.
    
    Відповідальність: Створення реальних підключень до зовнішніх систем.
    Викликається один раз при старті сервера (наприклад, у FastAPI lifespan або Flask context).
    """
    global _cassandra_session, _redis_client
    
    # Налаштування кластера Cassandra
    # Використовуємо хост та порт із централізованих налаштувань.
    cluster = Cluster([settings.cassandra_host], port=settings.cassandra_port)
    
    # GeventConnection забезпечує неблокуючий ввід/вивід, що критично 
    # для високонавантажених систем.
    cluster.connection_class = GeventConnection
    _cassandra_session = cluster.connect(settings.cassandra_keyspace)
    
    # Налаштування клієнта Redis
    # decode_responses=True дозволяє отримувати дані у форматі string замість bytes,
    # що спрощує роботу на рівні репозиторію.
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
    
    Реалізує Dependency Injection (DI):
    1. Створює конкретну реалізацію репозиторію (Cassandra).
    2. Створює реалізацію кешу (Redis).
    3. "Збирає" Use Case, передаючи йому ці залежності через конструктор.
    """
    review_repo = CassandraReviewRepository(_cassandra_session)
    cache_repo = RedisCache(_redis_client)
    
    # Повертаємо готовий до роботи об'єкт бізнес-логіки.
    return GetProductReviewsUseCase(review_repo, cache_repo, settings.cache_ttl)

def get_product_reviews_by_rating_use_case() -> GetProductReviewsByRatingUseCase:
    """
    Фабричний метод для Use Case фільтрації за рейтингом.
    
    Дотримується принципу Single Responsibility: цей метод знає ТІЛЬКИ 
    як правильно зібрати саме цей сценарій використання.
    """
    review_repo = CassandraReviewRepository(_cassandra_session)
    cache_repo = RedisCache(_redis_client)
    return GetProductReviewsByRatingUseCase(review_repo, cache_repo, settings.cache_ttl)

def get_customer_reviews_use_case() -> GetCustomerReviewsUseCase:
    """
    Фабричний метод для Use Case отримання відгуків конкретного клієнта.
    
    Навіть якщо інфраструктурні об'єкти (repo, cache) однакові для всіх 
    use cases, ми створюємо їх окремо для кожного запиту (або використовуємо DI-контейнер), 
    щоб забезпечити ізоляцію та легкість тестування.
    """
    review_repo = CassandraReviewRepository(_cassandra_session)
    cache_repo = RedisCache(_redis_client)
    return GetCustomerReviewsUseCase(review_repo, cache_repo, settings.cache_ttl)