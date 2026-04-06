from abc import ABC, abstractmethod
from typing import Optional, List
from .entities import Review

# intraface that get review and save it

#domain level - constructor, application - instruction

class ReviewRepository(ABC):
    @abstractmethod
    def save(self, review: Review) -> None:
        """Зберегти відгук у базу даних"""
        pass

    @abstractmethod
    def get_by_id(self, review_id: str) -> Optional[Review]:
        """Знайти відгук за його унікальним ID"""
        pass