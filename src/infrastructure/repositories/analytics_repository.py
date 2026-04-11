from cassandra.cluster import Cluster
from typing import List, Dict
from collections import Counter

class AnalyticsRepository:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('review_keyspace')

    def get_top_haters(self, limit: int = 5) -> List[Dict]:
        """Отримуємо користувачів з найбільшою кількістю 1-зіркових відгуків"""
        # Вибираємо тільки ID користувачів, де рейтинг 1
        query = "SELECT customer_id FROM reviews WHERE star_rating = 1 ALLOW FILTERING"
        rows = self.session.execute(query)
        
        # Рахуємо кількість згадок кожного customer_id
        counts = Counter(r.customer_id for r in rows)
        
        # Перетворюємо у список словників та сортуємо
        result = [
            {"customer_id": cid, "total_reviews": count} 
            for cid, count in counts.items()
        ]
        
        return sorted(result, key=lambda x: x["total_reviews"], reverse=True)[:limit]

    def get_top_products(self, limit: int = 5) -> List[Dict]:
        """Отримуємо топ товарів за кількістю відгуків та середнім рейтингом"""
        query = "SELECT product_id, star_rating FROM reviews LIMIT 2000"
        rows = self.session.execute(query)
        
        stats = {}
        for r in rows:
            if r.product_id not in stats:
                stats[r.product_id] = {"count": 0, "sum": 0}
            stats[r.product_id]["count"] += 1
            stats[r.product_id]["sum"] += r.star_rating
            
        result = []
        for pid, data in stats.items():
            result.append({
                "product_id": pid,
                "review_count": data["count"],
                "avg_rating": round(data["sum"] / data["count"], 2)
            })
            
        return sorted(result, key=lambda x: x["review_count"], reverse=True)[:limit]