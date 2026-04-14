from abc import ABC, abstractmethod
from typing import Optional
from .entities import Review, ProductReviews, CustomerReviews

class ReviewRepository(ABC):
    """
    Інтерфейс сховища (Repository) для роботи з відгуками.
    
    Призначення: Виступає як колекція об'єктів у пам'яті, приховуючи складність 
    запитів до бази даних. Це реалізація Dependency Inversion Principle (DIP), 
    що дозволяє тестувати бізнес-логіку за допомогою mock-об'єктів.
    """
    
    @abstractmethod
    def get_by_product(self, product_id: str) -> ProductReviews:
        """
        Отримує всі відгуки, пов'язані з конкретним товаром.
        
        Використовується для відображення сторінки продукту та розрахунку 
        його середнього рейтингу.
        """
        pass
    
    @abstractmethod
    def get_by_product_and_rating(self, product_id: str, rating: int) -> ProductReviews:
        """
        Фільтрує відгуки продукту за певною оцінкою (наприклад, тільки 5 зірок).
        
        Дозволяє реалізувати функціонал швидкої фільтрації на клієнтському 
        інтерфейсі без завантаження зайвих даних.
        """
        pass
    
    @abstractmethod
    def get_by_customer(self, customer_id: str) -> CustomerReviews:
        """
        Отримує історію відгуків конкретного покупця.
        
        Важливо для особистого кабінету користувача або аналізу поведінки 
        клієнтів (Customer Analytics).
        """
        pass

class CacheRepository(ABC):
    """
    Абстракція над системою кешування (наприклад, Redis або Memcached).
    
    Призначення: Забезпечує швидкий доступ до даних, що часто запитуються, 
    знижуючи навантаження на основну базу даних (Cassandra) та зменшуючи 
    час відгуку системи (latency).
    """
    
    @abstractmethod
    def get(self, key: str) -> Optional[dict]:
        """
        Намагається отримати дані з кешу за унікальним ключем.
        
        Returns:
            Словник з даними, якщо ключ знайдено, або None, якщо дані 
            відсутні або термін їх дії вичерпано (Cache Miss).
        """
        pass
    
    @abstractmethod
    def set(self, key: str, value: dict, ttl: int) -> None:
        """
        Зберігає дані в кеші з обмеженням за часом.
        
        Args:
            key: Унікальний ідентифікатор запису.
            value: Дані для збереження (зазвичай серіалізований об'єкт).
            ttl: Time To Live (час життя) у секундах, після якого запис 
                 буде автоматично видалено.
        """
        pass