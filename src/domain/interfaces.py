from abc import ABC, abstractmethod
from src.domain.entities import Review

# Domain interface (Contract)
class IReviewRepository(ABC):
    @abstractmethod
    def save(self, review: Review):
        """Save a review entity to the persistent storage"""
        pass