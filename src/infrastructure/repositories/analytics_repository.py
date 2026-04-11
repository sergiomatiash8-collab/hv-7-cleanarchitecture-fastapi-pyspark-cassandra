import os
from cassandra.cluster import Cluster, Session
from cassandra import ConsistencyLevel
from pydantic import BaseModel
from typing import List
import logging

logger = logging.getLogger(__name__)

class CustomerMetric(BaseModel):
    """DTO для результатів аналітики"""
    customer_id: int
    review_count: int

class ProductMetric(BaseModel):
    """DTO для результатів аналітики"""
    product_id: str
    review_count: int
    avg_rating: float = 0.0

class AnalyticsRepository:
    """Аналітичні запити - використовуємо pre-computed aggregates"""
    
    def __init__(self):
        host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
        self.keyspace = "review_keyspace"
        
        try:
            self.cluster = Cluster([host], port=9042)
            self.session = self.cluster.connect(self.keyspace)
            self._prepare_statements()
            logger.info(f"✅ Analytics connection to Cassandra: {host}")
        except Exception as e:
            logger.error(f"❌ Analytics connection failed: {e}")
            raise
    
    def _prepare_statements(self):
        """Готуємо CQL для аналітики"""
        
        # Запити до таблиці reviews_by_rating_month (для top haters/backers)
        self.top_haters_query = self.session.prepare("""
            SELECT customer_id, COUNT(*) as hater_count
            FROM reviews_by_rating_month
            WHERE year_month = ? AND star_rating IN (1, 2)
            LIMIT ?
        """)
        
        self.top_backers_query = self.session.prepare("""
            SELECT customer_id, COUNT(*) as backer_count
            FROM reviews_by_rating_month
            WHERE year_month = ? AND star_rating IN (4, 5)
            LIMIT ?
        """)
        
        # Запит до pre-computed таблиці
        self.top_products_query = self.session.prepare("""
            SELECT product_id, review_count
            FROM review_counts_by_product_month
            WHERE year_month = ?
            ORDER BY review_count DESC
            LIMIT ?
        """)
    
    def get_top_haters(self, period: str = None, limit: int = 10) -> List[CustomerMetric]:
        """
        Топ користувачів, які ставлять 1-2 зірки.
        
        Args:
            period: "2023-01" або None для останнього місяця
            limit: кількість результатів
        """
        if not period:
            # Якщо період не вказаний, використовуємо останній місяць
            from datetime import datetime, timedelta
            today = datetime.now()
            period = today.strftime("%Y-%m")
        
        try:
            rows = self.session.execute(self.top_haters_query, [period, limit])
            
            # Перетворюємо в DTO
            result = [
                CustomerMetric(customer_id=int(row.customer_id), review_count=row.hater_count)
                for row in rows
            ]
            
            logger.info(f"✅ Found {len(result)} top haters for {period}")
            return result
        except Exception as e:
            logger.error(f"❌ Error fetching top haters: {e}")
            return []
    
    def get_top_backers(self, period: str = None, limit: int = 10) -> List[CustomerMetric]:
        """
        Топ користувачів, які ставлять 4-5 зірок.
        
        Args:
            period: "2023-01" або None для останнього місяця
            limit: кількість результатів
        """
        if not period:
            from datetime import datetime
            today = datetime.now()
            period = today.strftime("%Y-%m")
        
        try:
            rows = self.session.execute(self.top_backers_query, [period, limit])
            
            result = [
                CustomerMetric(customer_id=int(row.customer_id), review_count=row.backer_count)
                for row in rows
            ]
            
            logger.info(f"✅ Found {len(result)} top backers for {period}")
            return result
        except Exception as e:
            logger.error(f"❌ Error fetching top backers: {e}")
            return []
    
    def get_top_products(self, period: str = None, limit: int = 10) -> List[ProductMetric]:
        """
        Топ товарів за кількістю відгуків.
        
        Args:
            period: "2023-01" або None для останнього місяця
            limit: кількість результатів
        """
        if not period:
            from datetime import datetime
            today = datetime.now()
            period = today.strftime("%Y-%m")
        
        try:
            rows = self.session.execute(self.top_products_query, [period, limit])
            
            result = [
                ProductMetric(product_id=row.product_id, review_count=row.review_count)
                for row in rows
            ]
            
            logger.info(f"✅ Found {len(result)} top products for {period}")
            return result
        except Exception as e:
            logger.error(f"❌ Error fetching top products: {e}")
            return []
    
    def close(self):
        """Закрити з'єднання"""
        if self.session:
            self.session.shutdown()
        if self.cluster and not self.cluster.is_shutdown:
            self.cluster.shutdown()
        logger.info("🔌 Analytics connection closed")