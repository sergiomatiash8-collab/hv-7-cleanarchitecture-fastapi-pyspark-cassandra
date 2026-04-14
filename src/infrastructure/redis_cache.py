import json
import redis
from typing import Optional
from ..domain.repositories import CacheRepository

class RedisCache(CacheRepository):
    """
    Реалізація інтерфейсу CacheRepository з використанням Redis.
    
    Відповідальність: Забезпечення швидкого доступу до даних через механізми 
    тимчасового зберігання (In-memory storage).
    
    Принцип SRP: Клас фокусується виключно на серіалізації, десеріалізації 
    та взаємодії з API Redis.
    """
    
    def __init__(self, client: redis.Redis):
        """
        Ініціалізація через готовий клієнт Redis.
        Це дозволяє керувати життєвим циклом з'єднання (Connection Pool) 
        ззовні цього класу.
        """
        self._client = client
    
    def get(self, key: str) -> Optional[dict]:
        """
        Отримує дані за ключем та конвертує їх назад у словник Python.
        
        Процес:
        1. Запит до Redis.
        2. Перевірка наявності (Cache Hit / Cache Miss).
        3. Десеріалізація з JSON рядка у об'єкт dict.
        
        Returns:
            Словник з даними або None, якщо ключ не існує або прострочений.
        """
        cached = self._client.get(key)
        if cached:
            # Redis повертає рядок (або bytes), який ми парсимо як JSON
            return json.loads(cached)
        return None
    
    def set(self, key: str, value: dict, ttl: int) -> None:
        """
        Зберігає дані в Redis із заданим терміном життя (TTL).
        
        Args:
            key: Унікальний ідентифікатор запису (напр. "product:123").
            value: Словник з даними для кешування.
            ttl: Час актуальності запису в секундах.
            
        Особливість реалізації:
        Використовується default=str у json.dumps для безпечної серіалізації 
        об'єктів, які не підтримуються стандартним JSON (наприклад, об'єкти date).
        """
        # setex (Set with Expiration) гарантує атомарність встановлення значення та TTL
        self._client.setex(key, ttl, json.dumps(value, default=str))