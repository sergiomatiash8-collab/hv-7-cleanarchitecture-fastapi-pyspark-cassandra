import os
from cassandra.cluster import Cluster, Session
from cassandra import ConsistencyLevel
from src.domain.entities import Review
from src.domain.interfaces import IReviewRepository
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class CassandraReviewRepository(IReviewRepository):
    """Правильна реалізація - БЕЗ ALLOW FILTERING!"""
    
    def __init__(self):
        host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
        self.keyspace = "review_keyspace"
        
        try:
            self.cluster = Cluster([host], port=9042)
            self.session = self.cluster.connect(self.keyspace)
            self._prepare_statements()
            logger.info(f"✅ Connected to Cassandra: {host}")
        except Exception as e:
            logger.error(f"❌ Cassandra connection failed: {e}")
            raise
    
    def _prepare_statements(self):
        """準備 CQL statements для всіх операцій"""
        
        # INSERT statements
        self.insert_by_product = self.session.prepare("""
            INSERT INTO reviews_by_product (
                product_id, review_id, customer_id, star_rating, 
                review_date, verified_purchase, review_body, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        self.insert_by_product_rating = self.session.prepare("""
            INSERT INTO reviews_by_product_rating (
                product_id, star_rating, review_id, customer_id,
                review_date, verified_purchase, review_body, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        self.insert_by_customer = self.session.prepare("""
            INSERT INTO reviews_by_customer (
                customer_id, review_id, product_id, star_rating,
                review_date, verified_purchase, review_body, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # SELECT statements
        self.select_by_product = self.session.prepare("""
            SELECT * FROM reviews_by_product WHERE product_id = ?
        """)
        self.select_by_product.consistency_level = ConsistencyLevel.ONE
        
        self.select_by_product_rating = self.session.prepare("""
            SELECT * FROM reviews_by_product_rating 
            WHERE product_id = ? AND star_rating = ?
        """)
        self.select_by_product_rating.consistency_level = ConsistencyLevel.ONE
        
        self.select_by_customer = self.session.prepare("""
            SELECT * FROM reviews_by_customer WHERE customer_id = ?
        """)
        self.select_by_customer.consistency_level = ConsistencyLevel.ONE
    
    def save(self, review: Review) -> bool:
        """Зберегти відгук у 3 таблиці одночасно (denormalization)"""
        try:
            # Таблиця 1: reviews_by_product
            self.session.execute(self.insert_by_product, (
                review.product_id,
                review.review_id,
                int(review.customer_id),
                review.star_rating,
                review.created_at.date() if isinstance(review.created_at, datetime) else review.created_at,
                True,  # verified_purchase - за замовчуванням True
                review.review_body,
                review.created_at
            ))
            
            # Таблиця 2: reviews_by_product_rating
            self.session.execute(self.insert_by_product_rating, (
                review.product_id,
                review.star_rating,
                review.review_id,
                int(review.customer_id),
                review.created_at.date() if isinstance(review.created_at, datetime) else review.created_at,
                True,
                review.review_body,
                review.created_at
            ))
            
            # Таблиця 3: reviews_by_customer
            self.session.execute(self.insert_by_customer, (
                int(review.customer_id),
                review.review_id,
                review.product_id,
                review.star_rating,
                review.created_at.date() if isinstance(review.created_at, datetime) else review.created_at,
                True,
                review.review_body,
                review.created_at
            ))
            
            logger.info(f"✅ Review {review.review_id} saved to 3 tables")
            return True
        except Exception as e:
            logger.error(f"❌ Error saving review: {e}")
            return False
    
    def get_by_product(self, product_id: str) -> list[Review]:
        """Отримати відгуки за товаром - БЕЗ ALLOW FILTERING!"""
        try:
            rows = self.session.execute(self.select_by_product, [product_id])
            result = [self._row_to_entity(row) for row in rows]
            logger.info(f"✅ Found {len(result)} reviews for product {product_id}")
            return result
        except Exception as e:
            logger.error(f"❌ Error fetching by product: {e}")
            return []
    
    def get_by_product_and_rating(self, product_id: str, rating: int) -> list[Review]:
        """✅ НОВЕ: Отримати відгуки за товаром + рейтингом (ВАЖЛИВО для ТЗ!)"""
        try:
            rows = self.session.execute(self.select_by_product_rating, [product_id, rating])
            result = [self._row_to_entity(row) for row in rows]
            logger.info(f"✅ Found {len(result)} reviews for product {product_id} with rating {rating}")
            return result
        except Exception as e:
            logger.error(f"❌ Error fetching by product and rating: {e}")
            return []
    
    def get_by_customer(self, customer_id: str) -> list[Review]:
        """Отримати відгуки за клієнтом"""
        try:
            rows = self.session.execute(self.select_by_customer, [int(customer_id)])
            result = [self._row_to_entity(row) for row in rows]
            logger.info(f"✅ Found {len(result)} reviews for customer {customer_id}")
            return result
        except Exception as e:
            logger.error(f"❌ Error fetching by customer: {e}")
            return []
    
    def get_all(self) -> list[Review]:
        """Отримати всі відгуки (для тестування)"""
        try:
            rows = self.session.execute("SELECT * FROM reviews_by_product LIMIT 100")
            return [self._row_to_entity(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Error fetching all reviews: {e}")
            return []
    
    @staticmethod
    def _row_to_entity(row) -> Review:
        """Конвертація DB row → Review entity"""
        return Review(
            review_id=row.review_id,
            product_id=row.product_id,
            customer_id=row.customer_id,
            star_rating=row.star_rating,
            review_body=row.review_body,
            created_at=row.created_at
        )
    
    def close(self):
        """Закрити з'єднання"""
        if self.session:
            self.session.shutdown()
        if self.cluster and not self.cluster.is_shutdown:
            self.cluster.shutdown()
        logger.info("🔌 Cassandra connection closed")