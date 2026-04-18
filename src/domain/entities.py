from dataclasses import dataclass
from datetime import date
from typing import Optional, List

@dataclass
class Review:
    review_id: str
    product_id: str
    customer_id: str
    star_rating: int
    review_date: date
    review_body: Optional[str] = None

@dataclass
class ProductReviews:
    product_id: str
    reviews: List[Review]
    
    @property
    def count(self) -> int:
        return len(self.reviews)

@dataclass
class CustomerReviews:
    customer_id: str
    reviews: List[Review]
    
    @property
    def count(self) -> int:
        return len(self.reviews)

@dataclass
class TopReviewedProduct:
    product_id: str
    review_count: int

@dataclass
class TopCustomer:
    customer_id: str
    review_count: int

@dataclass
class TopHater:
    customer_id: str
    bad_reviews_count: int

@dataclass
class TopBacker:
    customer_id: str
    good_reviews_count: int

@dataclass
class DatasetMetadata:
    columns: List[str]
    row_count: int
    source_path: str
    
    def __str__(self) -> str:
        return f"Dataset: {len(self.columns)} cols, {self.row_count} rows"

@dataclass
class ETLResult:
    success: bool
    operation: str
    metadata: DatasetMetadata
    output_path: Optional[str] = None
    rows_processed: int = 0
    error_message: str = ""
    duration_seconds: float = 0.0
    
    def log_summary(self) -> str:
        if self.success:
            return f"OK: {self.operation} ({self.rows_processed} rows)"
        return f"ERROR: {self.operation} - {self.error_message}"

@dataclass
class CassandraTableConfig:
    keyspace: str
    table: str
    columns: List[str]
    
    @property
    def full_name(self) -> str:
        return f"{self.keyspace}.{self.table}"