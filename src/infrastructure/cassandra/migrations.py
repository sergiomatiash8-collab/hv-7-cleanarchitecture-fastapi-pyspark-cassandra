from cassandra.cluster import Cluster
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CassandraMigrations:

    def __init__(self, session):
        self.session = session
        self.keyspace = "review_keyspace"

    def create_keyspace(self):
        cql = f"""
        CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}
        """
        self.session.execute(cql)
        self.session.set_keyspace(self.keyspace)
        logger.info("Keyspace created")

    def create_reviews_by_product_table(self):
        cql = f"""
        CREATE TABLE IF NOT EXISTS {self.keyspace}.reviews_by_product (
            product_id TEXT,
            review_id TEXT,
            customer_id BIGINT,
            star_rating INT,
            review_date DATE,
            verified_purchase BOOLEAN,
            review_body TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (product_id, review_id)
        )
        """
        self.session.execute(cql)
        logger.info("reviews_by_product created")

    def create_reviews_by_customer_table(self):
        cql = f"""
        CREATE TABLE IF NOT EXISTS {self.keyspace}.reviews_by_customer (
            customer_id BIGINT,
            review_id TEXT,
            product_id TEXT,
            star_rating INT,
            review_date DATE,
            verified_purchase BOOLEAN,
            review_body TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (customer_id, review_id)
        )
        """
        self.session.execute(cql)
        logger.info("reviews_by_customer created")

    def run_all_migrations(self):
        self.create_keyspace()
        self.create_reviews_by_product_table()
        self.create_reviews_by_customer_table()
        logger.info("All migrations completed")


if __name__ == "__main__":
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    CassandraMigrations(session).run_all_migrations()

    session.shutdown()
    cluster.shutdown()