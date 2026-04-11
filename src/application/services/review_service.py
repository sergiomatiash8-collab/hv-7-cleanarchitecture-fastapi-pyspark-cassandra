import uuid
from datetime import datetime
from typing import List, Optional
from src.domain.entities import Review
from src.domain.interfaces import IReviewRepository
from src.infrastructure.redis.client import RedisClient

class ReviewService:
    def __init__(self, repository: IReviewRepository, redis_client: RedisClient):
        self.repository = repository
        self.redis_client = redis_client

    def create_review(self, product_id: str, customer_id: str, rating: int, body: str) -> Review:
        """Створює відгук, пише в БД та інвалідує відповідний кеш."""
        new_review = Review(
            review_id=str(uuid.uuid4()),
            product_id=product_id,
            customer_id=customer_id,
            star_rating=rating,
            review_body=body,
            created_at=datetime.now()
        )
        self.repository.save(new_review)
        
        # Видаляємо старий кеш, щоб при наступному запиті дані оновилися
        self.redis_client.client.delete(f"reviews:product:{product_id}")
        self.redis_client.client.delete(f"reviews:customer:{customer_id}")
        self.redis_client.client.delete(f"reviews:product:{product_id}:rating:{rating}")
        
        return new_review

    def get_reviews_by_product(self, product_id: str) -> List[Review]:
        """Отримує відгуки за продуктом: спочатку Redis, потім Cassandra."""
        cache_key = f"reviews:product:{product_id}"
        
        cached_data = self.redis_client.get_cache(cache_key)
        if cached_data:
            print(f"🚀 [Redis] Cache hit for product: {product_id}")
            return [Review(**r) for r in cached_data]

        print(f"🏠 [Cassandra] Fetching reviews for product: {product_id}")
        reviews = self.repository.get_by_product(product_id)
        
        if reviews:
            self._save_to_cache(cache_key, reviews)
            
        return reviews

    def get_reviews_by_customer(self, customer_id: str) -> List[Review]:
        """Отримує відгуки за клієнтом: спочатку Redis, потім Cassandra."""
        cache_key = f"reviews:customer:{customer_id}"
        
        cached_data = self.redis_client.get_cache(cache_key)
        if cached_data:
            print(f"🚀 [Redis] Cache hit for customer: {customer_id}")
            return [Review(**r) for r in cached_data]

        print(f"🏠 [Cassandra] Fetching reviews for customer: {customer_id}")
        reviews = self.repository.get_by_customer(customer_id)
        
        if reviews:
            self._save_to_cache(cache_key, reviews)
            
        return reviews

    def get_reviews_by_product_and_rating(self, product_id: str, rating: int) -> List[Review]:
        """Отримує відгуки за продуктом та рейтингом (Пункт 66 ТЗ)."""
        cache_key = f"reviews:product:{product_id}:rating:{rating}"
        
        cached_data = self.redis_client.get_cache(cache_key)
        if cached_data:
            print(f"🚀 [Redis] Cache hit for product {product_id} with rating {rating}")
            return [Review(**r) for r in cached_data]

        print(f"🏠 [Cassandra] Fetching reviews for product {product_id} and rating {rating}")
        reviews = self.repository.get_by_product_and_rating(product_id, rating)
        
        if reviews:
            self._save_to_cache(cache_key, reviews)
            
        return reviews

    def _save_to_cache(self, key: str, reviews: List[Review]):
        """Внутрішній метод для серіалізації та збереження в Redis."""
        dict_reviews = []
        for r in reviews:
            r_dict = r.__dict__.copy()
            if isinstance(r_dict['created_at'], datetime):
                r_dict['created_at'] = r_dict['created_at'].isoformat()
            dict_reviews.append(r_dict)
        
        self.redis_client.set_cache(key, dict_reviews)