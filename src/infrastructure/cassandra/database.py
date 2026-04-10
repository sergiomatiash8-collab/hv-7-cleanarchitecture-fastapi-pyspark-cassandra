from cassandra.cluster import Cluster
from typing import Optional

class CassandraSession:
    def __init__(self, hosts: list[str], port: int = 9042):
        """
        Ініціалізує налаштування підключення до кластера Cassandra.
        """
        self.hosts = hosts
        self.port = port
        self.cluster: Optional[Cluster] = None
        self.session = None

    def connect(self):
        """
        Встановлює з'єднання з кластером і створює сесію для виконання запитів.
        """
        # Перевіряємо, чи ми вже не підключені, щоб не створювати зайвих з'єднань
        if not self.cluster:
            # Створюємо об'єкт кластера, вказуючи адреси серверів
            self.cluster = Cluster(self.hosts, port=self.port)
            # Підключаємося та отримуємо активну сесію
            self.session = self.cluster.connect()
            print(f"✅ Успішно підключено до Cassandra на хостах: {self.hosts}")
        
        return self.session

    def close(self):
        """
        Безпечно закриває з'єднання з базою даних, звільняючи ресурси.
        """
        if self.cluster:
            self.cluster.shutdown()
            print("🛑 З'єднання з Cassandra закрито.")

# Створюємо єдиний (глобальний) екземпляр підключення.
# Поки що вказуємо 'localhost', але пізніше, коли додамо Docker, 
# ми замінимо це на ім'я контейнера (наприклад, 'cassandra_db').
cassandra_db = CassandraSession(hosts=['localhost'])