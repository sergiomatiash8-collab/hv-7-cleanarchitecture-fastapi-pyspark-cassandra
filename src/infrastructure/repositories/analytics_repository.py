import os
from cassandra.cluster import Cluster
from pydantic import BaseModel
from typing import List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


# =========================
# DTO
# =========================

class CustomerMetric(BaseModel):
    customer_id: int
    review_count: int


class ProductMetric(BaseModel):
    product_id: str
    review_count: int
    avg_rating: float = 0.0


# =========================
# REPOSITORY
# =========================

class AnalyticsRepository:
    """
    Аналітика через pre-computed Cassandra tables.
    Без агрегацій на льоту (Cassandra best practice).
    """

    def __init__(self):
        host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
        self.keyspace = "review_keyspace"

        try:
            self.cluster = Cluster([host], port=9042)
            self.session = self.cluster.connect(self.keyspace)

            logger.info(f"✅ Connected to Cassandra analytics: {host}")

        except Exception as e:
            logger.error(f"❌ Analytics connection failed: {e}")
            raise

    # =========================================================
    # TOP HATERS (precomputed)
    # =========================================================
    def get_top_haters(self, period: str = None, limit: int = 10) -> List[CustomerMetric]:

        if not period:
            period = datetime.now().strftime("%Y-%m")

        query = """
            SELECT customer_id, hater_count
            FROM review_counts_by_customer_month
            WHERE year_month = ?
            LIMIT ?
        """

        try:
            rows = self.session.execute(query, [period, limit])

            return [
                CustomerMetric(
                    customer_id=int(r.customer_id),
                    review_count=r.hater_count
                )
                for r in rows
            ]

        except Exception as e:
            logger.error(f"❌ get_top_haters failed: {e}")
            return []

    # =========================================================
    # TOP BACKERS (precomputed)
    # =========================================================
    def get_top_backers(self, period: str = None, limit: int = 10) -> List[CustomerMetric]:

        if not period:
            period = datetime.now().strftime("%Y-%m")

        query = """
            SELECT customer_id, backer_count
            FROM review_counts_by_customer_month
            WHERE year_month = ?
            LIMIT ?
        """

        try:
            rows = self.session.execute(query, [period, limit])

            return [
                CustomerMetric(
                    customer_id=int(r.customer_id),
                    review_count=r.backer_count
                )
                for r in rows
            ]

        except Exception as e:
            logger.error(f"❌ get_top_backers failed: {e}")
            return []

    # =========================================================
    # TOP PRODUCTS (precomputed + safe sort fallback)
    # =========================================================
    def get_top_products(self, period: str = None, limit: int = 10) -> List[ProductMetric]:

        if not period:
            period = datetime.now().strftime("%Y-%m")

        query = """
            SELECT product_id, review_count
            FROM review_counts_by_product_month
            WHERE year_month = ?
            LIMIT ?
        """

        try:
            rows = self.session.execute(query, [period, limit])

            result = [
                ProductMetric(
                    product_id=r.product_id,
                    review_count=r.review_count
                )
                for r in rows
            ]

            # safety sort (because Cassandra doesn't guarantee ORDER BY)
            result.sort(key=lambda x: x.review_count, reverse=True)

            return result

        except Exception as e:
            logger.error(f"❌ get_top_products failed: {e}")
            return []

    # =========================================================
    # CLOSE
    # =========================================================
    def close(self):
        try:
            if self.session:
                self.session.shutdown()

            if self.cluster:
                self.cluster.shutdown()

            logger.info("🔌 Analytics connection closed")

        except Exception as e:
            logger.error(f"❌ Error closing analytics: {e}")