import redis
import json
import os

class RedisClient:
    def __init__(self):
        # Якщо в системі немає змінної REDIS_HOST, використовуємо 127.0.0.1 для Windows
        host = os.getenv("REDIS_HOST", "127.0.0.1")
        port = int(os.getenv("REDIS_PORT", 6379))
        
        # host має збігатися з іменем сервісу в docker-compose або бути 127.0.0.1
        self.client = redis.Redis(host=host, port=port, db=0, decode_responses=True)
        self.default_ttl = 300  # 5 хвилин у секундах
        
        # Перевірка підключення при ініціалізації
        try:
            self.client.ping()
            print(f"🚀 [Redis] Connected to {host}")
        except Exception as e:
            print(f"❌ [Redis] Connection error: {e}")

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