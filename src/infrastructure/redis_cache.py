import json
import redis
from typing import Optional

from src.domain.repositories import CacheRepository

class RedisCache(CacheRepository):
    """
    Implementation of the CacheRepository interface using Redis.
    
    Responsibility: Providing fast access to data through temporary 
    storage mechanisms (In-memory storage).
    """
    
    def __init__(self, client: redis.Redis):
        """
        Initialization via a ready-to-use Redis client.
        """
        self._client = client
    
    def get(self, key: str) -> Optional[dict]:
        """
        Retrieves data by key and converts it back into a Python dictionary.
        """
        cached = self._client.get(key)
        if cached:
            
            return json.loads(cached)
        return None
    
    def set(self, key: str, value: dict, ttl: int) -> None:
        """
        Stores data in Redis with a specified time-to-live (TTL).
        """
        # setex (Set with Expiration) ensures atomicity of setting the value and TTL
        self._client.setex(key, ttl, json.dumps(value, default=str))