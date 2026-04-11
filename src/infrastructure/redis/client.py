import redis
import json

class RedisClient:
    def __init__(self, host='redis_dev', port=6379, db=0):
        # host має збігатися з іменем сервісу в docker-compose
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.default_ttl = 300  # 5 хвилин у секундах [cite: 54, 76]

    def set_cache(self, key: str, value, ttl=None):
        """Зберігає дані в кеш як JSON рядок."""
        if ttl is None:
            ttl = self.default_ttl
        self.client.setex(key, ttl, json.dumps(value))

    def get_cache(self, key: str):
        """Отримує дані з кешу та десеріалізує JSON."""
        data = self.client.get(key)
        return json.loads(data) if data else None

    def exists(self, key: str) -> bool:
        return self.client.exists(key) > 0