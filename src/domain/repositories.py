from abc import ABC, abstractmethod
from typing import Optional, List
from .entities import (
    Review, ProductReviews, CustomerReviews, 
    TopReviewedProduct, TopCustomer, TopHater, TopBacker
)

class ReviewRepository(ABC):
    @abstractmethod
    def get_by_product(self, product_id: str) -> ProductReviews:
        pass
    
    @abstractmethod
    def get_by_product_and_rating(self, product_id: str, rating: int) -> ProductReviews:
        pass
    
    @abstractmethod
    def get_by_customer(self, customer_id: str) -> CustomerReviews:
        pass

    @abstractmethod
    def get_top_reviewed_products(self, n: int, start_date: str, end_date: str) -> List[TopReviewedProduct]:
        pass

    @abstractmethod
    def get_top_customers_verified(self, n: int, start_date: str, end_date: str) -> List[TopCustomer]:
        pass

    @abstractmethod
    def get_top_haters(self, n: int, start_date: str, end_date: str) -> List[TopHater]:
        pass

    @abstractmethod
    def get_top_backers(self, n: int, start_date: str, end_date: str) -> List[TopBacker]:
        pass

class CacheRepository(ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[dict]:
        pass
    
    @abstractmethod
    def set(self, key: str, value: dict, ttl: int) -> None:
        pass