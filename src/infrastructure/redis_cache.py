import json
import redis
from typing import Optional
# Виправлено на абсолютний імпорт
from src.domain.repositories import CacheRepository

class RedisCache(CacheRepository):
    """
    Реалізація інтерфейсу CacheRepository з використанням Redis.
    
    Відповідальність: Забезпечення швидкого доступу до даних через механізми 
    тимчасового зберігання (In-memory storage).
    """
    
    def __init__(self, client: redis.Redis):
        """
        Ініціалізація через готовий клієнт Redis.
        """
        self._client = client
    
    def get(self, key: str) -> Optional[dict]:
        """
        Отримує дані за ключем та конвертує їх назад у словник Python.
        """
        cached = self._client.get(key)
        if cached:
            # Redis повертає рядок (або bytes), який ми парсимо як JSON
            return json.loads(cached)
        return None
    
    def set(self, key: str, value: dict, ttl: int) -> None:
        """
        Зберігає дані в Redis із заданим терміном життя (TTL).
        """
        # setex (Set with Expiration) гарантує атомарність встановлення значення та TTL
        self._client.setex(key, ttl, json.dumps(value, default=str))