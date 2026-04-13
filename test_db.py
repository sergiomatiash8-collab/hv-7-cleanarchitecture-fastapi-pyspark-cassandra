import os
# Цей рядок МАЄ бути першим, він готує систему до роботи з gevent
from gevent import monkey; monkey.patch_all()

from cassandra.cluster import Cluster
from cassandra.io.geventreactor import GeventConnection

def test_connection():
    try:
        print("Спроба підключення до Cassandra...")
        
        # Створюємо кластер, явно вказуючи клас з'єднання Gevent
        cluster = Cluster(['127.0.0.1'], port=9042)
        cluster.connection_class = GeventConnection
        
        session = cluster.connect()
        
        # Перевіряємо версію
        row = session.execute("SELECT release_version FROM system.local").one()
        
        if row:
            print(f"✅ УСПІХ! Зв'язок є. Версія Cassandra: {row.release_version}")
            print("Тепер ми точно можемо переходити до FastAPI.")
        
        cluster.shutdown()
    except Exception as e:
        print(f"❌ Помилка: {e}")

if __name__ == "__main__":
    test_connection()