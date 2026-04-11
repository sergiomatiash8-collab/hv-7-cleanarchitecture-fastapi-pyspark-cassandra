from cassandra.cluster import Session
import logging

logger = logging.getLogger(__name__)

class CassandraMigrations:
    """Управління схемою Cassandra - CREATE TABLE statements"""
    
    def __init__(self, session: Session):
        self.session = session
        self.keyspace = "review_keyspace"
    
    def create_keyspace(self):
        """Крок 1: Създать keyspace"""
        cql = f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}
        """
        try:
            self.session.execute(cql)
            logger.info(f"✅ Keyspace '{self.keyspace}' created")
        except Exception as e:
            logger.error(f"❌ Error creating keyspace: {e}")
            raise
    
    def create_reviews_by_product_table(self):
        """Таблиця 1: Отримати всі відгуки за товаром (IMPORTANT: БЕЗ ALLOW FILTERING!)"""
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
            ) WITH CLUSTERING ORDER BY (review_id ASC)
        """
        self.session.execute(cql)
        logger.info("✅ Table reviews_by_product created")
    
    def create_reviews_by_product_rating_table(self):
        """Таблиця 2: Отримати відгуки за товаром + рейтингом (ВАЖЛИВО!)"""
        cql = f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.reviews_by_product_rating (
                product_id TEXT,
                star_rating INT,
                review_id TEXT,
                customer_id BIGINT,
                review_date DATE,
                verified_purchase BOOLEAN,
                review_body TEXT,
                created_at TIMESTAMP,
                PRIMARY KEY ((product_id, star_rating), review_id)
            ) WITH CLUSTERING ORDER BY (review_id ASC)
        """
        self.session.execute(cql)
        logger.info("✅ Table reviews_by_product_rating created")
    
    def create_reviews_by_customer_table(self):
        """Таблиця 3: Отримати всі відгуки за клієнтом"""
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
            ) WITH CLUSTERING ORDER BY (review_id ASC)
        """
        self.session.execute(cql)
        logger.info("✅ Table reviews_by_customer created")
    
    def create_reviews_by_month_table(self):
        """Таблиця 4: Reviews для аналітики за період (для time-series)"""
        cql = f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.reviews_by_month (
                year_month TEXT,
                product_id TEXT,
                review_id TEXT,
                customer_id BIGINT,
                star_rating INT,
                verified_purchase BOOLEAN,
                review_date DATE,
                PRIMARY KEY ((year_month), product_id, review_id)
            ) WITH CLUSTERING ORDER BY (product_id ASC, review_id ASC)
        """
        self.session.execute(cql)
        logger.info("✅ Table reviews_by_month created")
    
    def create_reviews_by_rating_month_table(self):
        """Таблиця 5: Reviews по рейтингу + місяць (для topHaters/Backers)"""
        cql = f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.reviews_by_rating_month (
                year_month TEXT,
                star_rating INT,
                review_id TEXT,
                customer_id BIGINT,
                product_id TEXT,
                verified_purchase BOOLEAN,
                review_date DATE,
                PRIMARY KEY ((year_month, star_rating), review_id)
            ) WITH CLUSTERING ORDER BY (review_id ASC)
        """
        self.session.execute(cql)
        logger.info("✅ Table reviews_by_rating_month created")
    
    def create_aggregated_counts_table(self):
        """Таблиця 6: Pre-computed aggregations (за товарами та клієнтами)"""
        cql = f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.review_counts_by_product_month (
                year_month TEXT,
                product_id TEXT,
                review_count INT,
                avg_rating DECIMAL,
                PRIMARY KEY (year_month, product_id)
            ) WITH CLUSTERING ORDER BY (product_id ASC)
        """
        self.session.execute(cql)
        logger.info("✅ Table review_counts_by_product_month created")
        
        # Додаткова таблиця для клієнтів
        cql2 = f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.review_counts_by_customer_month (
                year_month TEXT,
                customer_id BIGINT,
                verified_count INT,
                hater_count INT,
                backer_count INT,
                PRIMARY KEY (year_month, customer_id)
            ) WITH CLUSTERING ORDER BY (customer_id ASC)
        """
        self.session.execute(cql2)
        logger.info("✅ Table review_counts_by_customer_month created")
    
    def run_all_migrations(self):
        """Виконати ВСІ міграції по порядку"""
        try:
            self.create_keyspace()
            self.create_reviews_by_product_table()
            self.create_reviews_by_product_rating_table()
            self.create_reviews_by_customer_table()
            self.create_reviews_by_month_table()
            self.create_reviews_by_rating_month_table()
            self.create_aggregated_counts_table()
            logger.info("✅ All migrations completed successfully!")
            return True
        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            raise


# --- ВИКОРИСТАННЯ ---
if __name__ == "__main__":
    from cassandra.cluster import Cluster
    
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    
    migrations = CassandraMigrations(session)
    migrations.run_all_migrations()
    
    session.shutdown()
    cluster.shutdown()