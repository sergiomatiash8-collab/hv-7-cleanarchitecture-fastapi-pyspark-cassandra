from abc import ABC, abstractmethod
from typing import List, Any
from .entities import DatasetMetadata, CassandraTableConfig

class DataFrame(ABC):
    """
    Абстракція над табличною структурою даних.
    
    Призначення: Декуплеризація (роз'єднання) бізнес-логіки від конкретних бібліотек 
    обробки даних (Apache Spark, Pandas, Polars). Це дозволяє змінювати рушій 
    обробки без переписування всього ETL-пайплайну.
    """
    pass


class DataReader(ABC):
    """
    Інтерфейс (Порт) для операцій читання даних (Extract).
    
    Відповідальність: Визначення стандарту отримання даних з різних форматів 
    та джерел, забезпечуючи уніфікований вихід у вигляді об'єкта DataFrame.
    """
    
    @abstractmethod
    def read(self, path: str, **options) -> DataFrame:
        """
        Зчитує дані за вказаним шляхом.
        
        Args:
            path: Локація файлу або ресурс (S3 bucket, локальна папка, URL).
            options: Словник параметрів, специфічних для формату (напр. сепаратор для CSV).
        
        Returns:
            Об'єкт, що реалізує інтерфейс DataFrame.
        """
        pass
    
    @abstractmethod
    def get_metadata(self, df: DataFrame) -> DatasetMetadata:
        """
        Аналізує DataFrame для отримання статистичної та структурної інформації.
        
        Використовується для валідації даних після зчитування та логування 
        стану системи перед початком трансформацій.
        """
        pass


class DataWriter(ABC):
    """
    Інтерфейс (Порт) для операцій збереження даних (Load).
    
    Відповідальність: Абстрагування процесу персистентності (запису) даних 
    у файлові системи або бази даних.
    """
    
    @abstractmethod
    def write(self, df: DataFrame, path: str, mode: str = "overwrite", **options) -> None:
        """
        Записує вміст DataFrame у цільове призначення.
        
        Args:
            df: Дані для збереження.
            path: Місце запису (шлях до файлу, назва таблиці).
            mode: Стратегія запису: 'overwrite' (перезапис) або 'append' (додавання).
            options: Додаткові параметри драйвера запису.
        """
        pass


class DataTransformer(ABC):
    """
    Інтерфейс (Порт) для трансформації даних (Transform).
    
    Відповідальність: Містить визначення методів для очищення, збагачення 
    та підготовки даних відповідно до бізнес-вимог. Це "мозок" ETL-процесу.
    """
    
    @abstractmethod
    def clean_reviews(self, df: DataFrame) -> DataFrame:
        """
        Виконує комплексне очищення сирих даних відгуків.
        
        Сюди входить: 
        - Приведення типів (Data Casting).
        - Обробка відсутніх значень (Handling Nulls).
        - Генерація похідних ознак (Feature Engineering), наприклад, year_month.
        """
        pass
    
    @abstractmethod
    def select_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Проекція даних: залишає лише необхідний набір колонок.
        Використовується для оптимізації пам'яті та відповідності схемі цільової БД.
        """
        pass


class CassandraLoader(ABC):
    """
    Спеціалізований інтерфейс для завантаження даних у NoSQL БД Cassandra.
    
    Виділений в окремий клас, оскільки робота з Cassandra вимагає специфічних 
    метаданих (Keyspace, Table Configuration) та механізмів консистентності, 
    які виходять за рамки звичайного DataWriter.
    """
    
    @abstractmethod
    def load_to_table(
        self, 
        df: DataFrame, 
        table_config: CassandraTableConfig,
        mode: str = "append"
    ) -> int:
        """
        Виконує завантаження даних у конкретну таблицю Cassandra.
        
        Args:
            df: Оброблений набір даних.
            table_config: Конфігурація цільової таблиці (схема, ключі).
            mode: Режим запису (зазвичай 'append' для розподілених БД).
            
        Returns:
            Кількість успішно завантажених записів для подальшої перевірки (Audit).
        """
        pass