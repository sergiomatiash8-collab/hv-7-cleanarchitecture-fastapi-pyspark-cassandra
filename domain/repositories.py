from abc import ABC, abstractmethod
from typing import Optional
from .entities import Review, ProductReviews, CustomerReviews

class ReviewRepository(ABC):
    """Інтерфейс репозиторію для відгуків (DIP - залежимо від абстракції)"""
    
    @abstractmethod
    def get_by_product(self, product_id: str) -> ProductReviews:
        pass
    
    @abstractmethod
    def get_by_product_and_rating(self, product_id: str, rating: int) -> ProductReviews:
        pass
    
    @abstractmethod
    def get_by_customer(self, customer_id: str) -> CustomerReviews:
        pass

class CacheRepository(ABC):
    """Інтерфейс для кешування"""
    
    @abstractmethod
    def get(self, key: str) -> Optional[dict]:
        pass
    
    @abstractmethod
    def set(self, key: str, value: dict, ttl: int) -> None:
        pass