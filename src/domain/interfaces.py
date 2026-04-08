from abc import ABC, abstractmethod
from typing import List
from src.domain.entities import Review

class IReviewRepository(ABC):
    @abstractmethod
    def save(self, review: Review):
        pass

    @abstractmethod
    def get_all(self) -> List[Review]:
        """Fetch all reviews from the storage"""
        pass