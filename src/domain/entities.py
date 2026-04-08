from dataclasses import dataclass
from datetime import datetime

# domain entity
@dataclass
class Review:
    review_id: str
    product_id: str
    customer_id: str
    star_rating: int
    review_body: str
    created_at: datetime  # Unified naming