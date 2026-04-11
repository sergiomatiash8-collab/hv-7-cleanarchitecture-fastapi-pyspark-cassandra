from cassandra.cluster import Cluster
from typing import Optional


class CassandraSession:
    def __init__(self, hosts: list[str], port: int = 9042):
        self.hosts = hosts
        self.port = port
        self.cluster: Optional[Cluster] = None
        self.session = None

    def connect(self):
        if not self.cluster:
            self.cluster = Cluster(self.hosts, port=self.port)
            self.session = self.cluster.connect()
            print(f"Connected to Cassandra: {self.hosts}")
        return self.session

    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            print("Cassandra connection closed")


# IMPORTANT: Docker service name (НЕ localhost)
cassandra_db = CassandraSession(hosts=["hv7-cassandra-dev"])