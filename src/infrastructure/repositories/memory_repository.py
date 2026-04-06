from typing import Optional, List
from src.domain.entities import Review
from src.domain.interfaces import ReviewRepository

#process of doing

class MemoryReviewRepository(ReviewRepository):
    def __init__(self):
       
        self._reviews: List[Review] = []

    def save(self, review: Review) -> None:
        self._reviews.append(review)
        print(f"[Infrastructure] Відгук {review.review_id} збережено в пам'ять!")

    def get_by_id(self, review_id: str) -> Optional[Review]:
        for r in self._reviews:
            if r.review_id == review_id:
                return r
        return None