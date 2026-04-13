from dataclasses import dataclass
from datetime import date
from typing import Optional

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
    reviews: list[Review]
    
    @property
    def count(self) -> int:
        return len(self.reviews)

@dataclass
class CustomerReviews:
    customer_id: str
    reviews: list[Review]
    
    @property
    def count(self) -> int:
        return len(self.reviews)

@dataclass
class DatasetMetadata:
    """
    Метадані про датасет
    Використовується для логування та валідації
    """
    columns: List[str]          # Список колонок
    row_count: int              # Кількість рядків
    source_path: str            # Звідки читали
    
    def __str__(self) -> str:
        return f"Dataset: {len(self.columns)} cols, {self.row_count} rows"


@dataclass
class ETLResult:
    """
    Результат будь-якого ETL процесу
    Уніфікований формат для всіх операцій
    """
    success: bool                      # Чи успішно
    operation: str                     # Назва операції (CSV→Parquet, Load to Cassandra)
    metadata: DatasetMetadata          # Інфо про дані
    output_path: Optional[str] = None  # Куди записали (якщо файл)
    rows_processed: int = 0            # Скільки рядків оброблено
    error_message: str = ""            # Помилка (якщо є)
    duration_seconds: float = 0.0      # Час виконання
    
    def log_summary(self) -> str:
        """Форматований вивід результату"""
        if self.success:
            return (
                f"✅ {self.operation} SUCCESS\n"
                f"   Rows: {self.rows_processed}\n"
                f"   Time: {self.duration_seconds:.2f}s\n"
                f"   Output: {self.output_path or 'Cassandra'}"
            )
        else:
            return (
                f"❌ {self.operation} FAILED\n"
                f"   Error: {self.error_message}"
            )


@dataclass
class CassandraTableConfig:
    """
    Конфігурація для запису в Cassandra таблицю
    Потрібно для load_to_cassandra.py (запис у 3 таблиці)
    """
    keyspace: str               # amazon_reviews
    table: str                  # reviews_by_customer, reviews_by_product...
    columns: List[str]          # Які колонки записувати
    
    @property
    def full_name(self) -> str:
        return f"{self.keyspace}.{self.table}"