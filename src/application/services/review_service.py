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
        new_review = Review(
            review_id=str(uuid.uuid4()),
            product_id=product_id,
            customer_id=customer_id,
            star_rating=rating,
            review_body=body,
            created_at=datetime.now()
        )
        self.repository.save(new_review)
        # При створенні нового відгуку можна інвалідувати кеш для цього товару/клієнта
        self.redis_client.client.delete(f"reviews:product:{product_id}")
        self.redis_client.client.delete(f"reviews:customer:{customer_id}")
        return new_review

    def get_reviews_by_product(self, product_id: str) -> List[Review]:
        cache_key = f"reviews:product:{product_id}"
        
        # 1. Спроба взяти з кешу
        cached_data = self.redis_client.get_cache(cache_key)
        if cached_data:
            print(f"🚀 [Redis] Cache hit for product: {product_id}")
            return [Review(**r) for r in cached_data]

        # 2. Якщо в кеші немає — йдемо в Cassandra
        print(f"🏠 [Cassandra] Fetching reviews for product: {product_id}")
        reviews = self.repository.get_by_product(product_id)
        
        # 3. Зберігаємо в кеш на 300 секунд (5 хв)
        if reviews:
            # Перетворюємо об'єкти Review на словники для JSON
            dict_reviews = [r.__dict__ for r in reviews]
            # Обробка datetime для JSON серіалізації
            for r in dict_reviews:
                r['created_at'] = r['created_at'].isoformat()
            self.redis_client.set_cache(cache_key, dict_reviews)
            
        return reviews

    def get_reviews_by_customer(self, customer_id: str) -> List[Review]:
        cache_key = f"reviews:customer:{customer_id}"
        cached_data = self.redis_client.get_cache(cache_key)
        
        if cached_data:
            print(f"🚀 [Redis] Cache hit for customer: {customer_id}")
            return [Review(**r) for r in cached_data]

        reviews = self.repository.get_by_customer(customer_id)
        
        if reviews:
            dict_reviews = [r.__dict__ for r in reviews]
            for r in dict_reviews:
                r['created_at'] = r['created_at'].isoformat()
            self.redis_client.set_cache(cache_key, dict_reviews)
            
        return reviews