from dataclasses import dataclass
from datetime import date
from typing import Optional, List

@dataclass
class Review:
    """
    Представляє одиничний відгук про товар.
    Це базовий об'єкт даних (DTO), який містить всю необхідну інформацію 
    про транзакцію відгуку між клієнтом та продуктом.
    """
    review_id: str      # Унікальний ідентифікатор відгуку (Primary Key в джерелі)
    product_id: str     # ID товару, до якого залишено відгук (Partition key для деяких таблиць)
    customer_id: str    # ID користувача, який залишив відгук
    star_rating: int    # Оцінка від 1 до 5
    review_date: date   # Дата публікації відгуку
    review_body: Optional[str] = None  # Текстовий зміст відгуку (може бути порожнім)

@dataclass
class ProductReviews:
    """
    Агрегатор відгуків, згрупованих за продуктом.
    Використовується для аналізу популярності та рейтингу конкретного товару.
    """
    product_id: str      # Ідентифікатор продукту, для якого зібрано список
    reviews: list[Review] # Колекція всіх об'єктів Review, пов'язаних з цим товаром
    
    @property
    def count(self) -> int:
        """Повертає загальну кількість відгуків для даного продукту."""
        return len(self.reviews)

@dataclass
class CustomerReviews:
    """
    Агрегатор відгуків, згрупованих за клієнтом.
    Дозволяє відстежувати активність конкретного користувача та його вподобання.
    """
    customer_id: str     # Ідентифікатор клієнта
    reviews: list[Review] # Колекція всіх відгуків, які написав цей клієнт
    
    @property
    def count(self) -> int:
        """Повертає загальну кількість відгуків, залишених цим клієнтом."""
        return len(self.reviews)

@dataclass
class DatasetMetadata:
    """
    Зберігає статистичні та технічні дані про набір даних.
    Служить для забезпечення цілісності даних (Data Integrity) та 
    відстеження походження даних (Data Lineage) під час ETL-процесів.
    """
    columns: List[str]          # Назви колонок, які присутні в обробленому датасеті
    row_count: int              # Фактична кількість рядків (записів) у наборі
    source_path: str            # Шлях до вхідного файлу або назва джерела даних
    
    def __str__(self) -> str:
        """Повертає коротке текстове резюме про стан датасету для швидкого логування."""
        return f"Dataset: {len(self.columns)} cols, {self.row_count} rows"


@dataclass
class ETLResult:
    """
    Контейнер для результатів виконання будь-якого етапу ETL (Extract, Transform, Load).
    Забезпечує уніфікований інтерфейс для моніторингу успішності операцій
    та збору метрик продуктивності.
    """
    success: bool                       # Статус операції: True, якщо все пройшло без критичних помилок
    operation: str                      # Опис дії, що виконувалась (наприклад, "Convert CSV to Parquet")
    metadata: DatasetMetadata           # Детальні дані про оброблені записи
    output_path: Optional[str] = None   # Місце зберігання результату (шлях до файлу або назва БД)
    rows_processed: int = 0             # Кількість успішно оброблених/записаних рядків
    error_message: str = ""             # Опис помилки у разі success=False
    duration_seconds: float = 0.0       # Час виконання операції для аналізу продуктивності
    
    def log_summary(self) -> str:
        """
        Формує людиночитаний звіт про результати операції.
        Використовує візуальні індикатори (✅/❌) для швидкого аналізу логів.
        """
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
    Об'єкт конфігурації для цільових таблиць у NoSQL БД Cassandra.
    Визначає структуру запису та логічне групування даних (keyspace).
    """
    keyspace: str                # Назва простору ключів (логічна база даних у Cassandra)
    table: str                   # Назва конкретної таблиці для запису
    columns: List[str]           # Перелік колонок, які мають бути імпортовані в цю таблицю
    
    @property
    def full_name(self) -> str:
        """
        Повертає повне кваліфіковане ім'я таблиці у форматі keyspace.table.
        Використовується в CQL запитах (Cassandra Query Language).
        """
        return f"{self.keyspace}.{self.table}"