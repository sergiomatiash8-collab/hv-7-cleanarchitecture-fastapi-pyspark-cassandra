from abc import ABC, abstractmethod
from typing import List, Any
from .entities import DatasetMetadata, CassandraTableConfig

class DataFrame(ABC):
    """
    Абстракція над DataFrame
    Дозволяє не залежати від конкретної реалізації (Spark/Pandas/Polars)
    """
    pass


class DataReader(ABC):
    """
    Порт для читання даних з різних джерел
    
    Реалізації:
    - SparkCSVReader (читає CSV через Spark)
    - SparkParquetReader (читає Parquet через Spark)
    """
    
    @abstractmethod
    def read(self, path: str, **options) -> DataFrame:
        """
        Читає дані з файлу
        
        Args:
            path: Шлях до файлу
            options: Додаткові опції (header=True, inferSchema=True...)
        
        Returns:
            DataFrame з даними
        """
        pass
    
    @abstractmethod
    def get_metadata(self, df: DataFrame) -> DatasetMetadata:
        """
        Витягує метадані з DataFrame
        
        Returns:
            DatasetMetadata (колонки, кількість рядків)
        """
        pass


class DataWriter(ABC):
    """
    Порт для запису даних
    
    Реалізації:
    - SparkParquetWriter (записує Parquet)
    - SparkCassandraWriter (записує в Cassandra)
    """
    
    @abstractmethod
    def write(self, df: DataFrame, path: str, mode: str = "overwrite", **options) -> None:
        """
        Записує дані
        
        Args:
            df: DataFrame з даними
            path: Куди записати (файл або таблиця)
            mode: append/overwrite
            options: Додаткові параметри
        """
        pass


class DataTransformer(ABC):
    """
    Порт для трансформацій даних
    
    Використовується в load_to_cassandra.py для:
    - Очищення даних (trim, cast)
    - Додавання колонок (year_month)
    - Фільтрації (isNotNull)
    """
    
    @abstractmethod
    def clean_reviews(self, df: DataFrame) -> DataFrame:
        """
        Очищує дані відгуків:
        - Конвертує типи
        - Видаляє невалідні дати
        - Додає year_month
        """
        pass
    
    @abstractmethod
    def select_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """Вибирає потрібні колонки"""
        pass


class CassandraLoader(ABC):
    """
    Порт для запису в Cassandra
    Окремо, бо тут специфічна логіка (keyspace, table)
    """
    
    @abstractmethod
    def load_to_table(
        self, 
        df: DataFrame, 
        table_config: CassandraTableConfig,
        mode: str = "append"
    ) -> int:
        """
        Завантажує дані в Cassandra таблицю
        
        Returns:
            Кількість записаних рядків
        """
        pass