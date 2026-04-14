from cassandra.cluster import Cluster, Session
from cassandra.io.geventreactor import GeventConnection
from typing import Optional
from ..domain.repositories import ReviewRepository
from ..domain.entities import Review, ProductReviews, CustomerReviews
from datetime import date

class CassandraReviewRepository(ReviewRepository):
    """
    Конкретна реалізація інтерфейсу ReviewRepository для Apache Cassandra.
    
    Відповідальність: Інкапсуляція CQL-запитів (Cassandra Query Language) та 
    забезпечення зв'язку між фізичною схемою таблиць і доменною моделлю.
    
    Принцип SRP: Клас відповідає тільки за витяг та мапінг даних з Cassandra.
    """
    
    def __init__(self, session: Session):
        """
        Ініціалізація через активну сесію Cassandra.
        Використовується Dependency Injection для можливості тестування.
        """
        self._session = session
    
    def get_by_product(self, product_id: str) -> ProductReviews:
        """
        Отримує відгуки з таблиці, оптимізованої для пошуку за продуктом.
        Така таблиця в Cassandra зазвичай має product_id як Partition Key.
        """
        query = "SELECT * FROM reviews_by_product WHERE product_id = %s"
        # Використання параметризованих запитів для запобігання CQL-ін'єкціям
        rows = self._session.execute(query, [product_id])
        
        # Перетворення кожного рядка результату в об'єкт Review
        reviews = [self._row_to_review(row) for row in rows]
        return ProductReviews(product_id=product_id, reviews=reviews)
    
    def get_by_product_and_rating(self, product_id: str, rating: int) -> ProductReviews:
        """
        Отримує відгуки продукту та фільтрує їх за рейтингом.
        
        Примітка щодо продуктивності: У Cassandra фільтрація за неключовими 
        полями (без ALLOW FILTERING) обмежена, тому тут обрана стратегія 
        фільтрації на рівні додатку (In-memory filtering).
        """
        all_reviews = self.get_by_product(product_id)
        filtered = [r for r in all_reviews.reviews if r.star_rating == rating]
        return ProductReviews(product_id=product_id, reviews=filtered)
    
    def get_by_customer(self, customer_id: str) -> CustomerReviews:
        """
        Отримує відгуки з денормалізованої таблиці reviews_by_customer.
        Відповідає принципу "одна таблиця на один запит" у NoSQL моделюванні.
        """
        query = "SELECT * FROM reviews_by_customer WHERE customer_id = %s"
        rows = self._session.execute(query, [customer_id])
        
        reviews = [self._row_to_review(row) for row in rows]
        return CustomerReviews(customer_id=customer_id, reviews=reviews)
    
    @staticmethod
    def _row_to_review(row) -> Review:
        """
        Допоміжний метод-мапер (Data Mapper).
        
        Перетворює сирий об'єкт рядка (Row), отриманий від драйвера, 
        у сутність Review, яка належить доменному рівню.
        
        Використання getattr дозволяє безпечно обробляти ситуації, коли 
        певні колонки можуть бути відсутні в результатах запиту.
        """
        return Review(
            review_id=row.review_id,
            product_id=row.product_id,
            # customer_id та review_body можуть бути опціональними залежно від таблиці
            customer_id=getattr(row, 'customer_id', ''),
            star_rating=row.star_rating,
            review_date=row.review_date,
            review_body=getattr(row, 'review_body', None)
        )