from dataclasses import dataclass
from datetime import datetime
from typing import Optional

#describing the class that saves data - only data structure

@dataclass(frozen=True)
class Review:
    review_id: str
    product_id: str
    customer_id: str
    star_rating: int
    review_body: str
    review_date: datetime