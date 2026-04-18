from abc import ABC, abstractmethod
from typing import List, Any
from .entities import DatasetMetadata, CassandraTableConfig

class DataFrame(ABC):
    pass

class DataReader(ABC):
    @abstractmethod
    def read(self, path: str, **options) -> DataFrame:
        pass
    
    @abstractmethod
    def get_metadata(self, df: DataFrame) -> DatasetMetadata:
        pass

class DataWriter(ABC):
    @abstractmethod
    def write(self, df: DataFrame, path: str, mode: str = "overwrite", **options) -> None:
        pass

class DataTransformer(ABC):
    @abstractmethod
    def clean_reviews(self, df: DataFrame) -> DataFrame:
        pass
    
    @abstractmethod
    def select_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        pass

class CassandraLoader(ABC):
    @abstractmethod
    def load_to_table(
        self, 
        df: DataFrame, 
        table_config: CassandraTableConfig,
        mode: str = "append"
    ) -> int:
        pass