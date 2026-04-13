import json
import redis
from typing import Optional
from ..domain.repositories import CacheRepository

class RedisCache(CacheRepository):
    """Реалізація кешу через Redis (SRP - тільки кешування)"""
    
    def __init__(self, client: redis.Redis):
        self._client = client
    
    def get(self, key: str) -> Optional[dict]:
        cached = self._client.get(key)
        if cached:
            return json.loads(cached)
        return None
    
    def set(self, key: str, value: dict, ttl: int) -> None:
        self._client.setex(key, ttl, json.dumps(value, default=str))