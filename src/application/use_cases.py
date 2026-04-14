from typing import Optional
from ..domain.repositories import ReviewRepository, CacheRepository
from ..domain.entities import ProductReviews, CustomerReviews

class GetProductReviewsUseCase:
    """
    Сценарій використання: Отримання відгуків про продукт.
    
    Реалізує патерн **Cache-Aside**: 
    1. Перевірка наявності даних у швидкому кеші.
    2. Якщо немає — запит до основної бази та оновлення кешу.
    
    Дотримується SRP (Single Responsibility Principle): клас відповідає 
    виключно за координацію процесу отримання відгуків для продукту.
    """
    
    def __init__(
        self, 
        review_repo: ReviewRepository, 
        cache_repo: CacheRepository,
        cache_ttl: int = 60
    ):
        """
        Ін'єкція залежностей через конструктор.
        Ми залежимо від абстракцій (Repository), а не від конкретних БД.
        """
        self._review_repo = review_repo
        self._cache_repo = cache_repo
        self._cache_ttl = cache_ttl
    
    def execute(self, product_id: str) -> dict:
        """
        Головна точка входу в сценарій.
        
        Args:
            product_id: Ідентифікатор товару.
            
        Returns:
            Словник (DTO), готовий до серіалізації в JSON.
        """
        # Формуємо унікальний ключ для кешування результату
        cache_key = f"product:{product_id}"
        
        # Спроба отримати дані з кешу (Fast Path)
        cached = self._cache_repo.get(cache_key)
        if cached:
            return cached
        
        # Якщо в кеші порожньо — звертаємось до репозиторію (Slow Path)
        product_reviews = self._review_repo.get_by_product(product_id)
        
        # Трансформація об'єктів доменної моделі у формат відповіді
        response = self._format_response(product_reviews)
        
        # Оновлення кешу для наступних запитів
        self._cache_repo.set(cache_key, response, self._cache_ttl)
        
        return response
    
    @staticmethod
    def _format_response(product_reviews: ProductReviews) -> dict:
        """
        Мапінг (перетворення) доменної сутності в словник.
        Приховує внутрішню структуру об'єкта Review від зовнішнього світу.
        """
        return {
            "product_id": product_reviews.product_id,
            "count": product_reviews.count,
            "reviews": [
                {
                    "review_id": r.review_id,
                    "star_rating": r.star_rating,
                    "review_date": str(r.review_date), # Приведення дати до рядка для JSON
                    "review_body": r.review_body
                }
                for r in product_reviews.reviews
            ]
        }

class GetProductReviewsByRatingUseCase:
    """
    Сценарій використання: Фільтрація відгуків за рейтингом.
    
    Дозволяє отримати вузькоспеціалізовану вибірку (наприклад, тільки негативні відгуки),
    мінімізуючи передачу даних по мережі.
    """
    
    def __init__(
        self, 
        review_repo: ReviewRepository, 
        cache_repo: CacheRepository,
        cache_ttl: int = 60
    ):
        self._review_repo = review_repo
        self._cache_repo = cache_repo
        self._cache_ttl = cache_ttl
    
    def execute(self, product_id: str, rating: int) -> dict:
        """
        Виконує пошук відгуків з урахуванням фільтра за зірками.
        Кешує результат окремо для кожної комбінації ID та рейтингу.
        """
        cache_key = f"product:{product_id}:rating:{rating}"
        
        cached = self._cache_repo.get(cache_key)
        if cached:
            return cached
        
        # Виклик специфічного методу репозиторію для фільтрації на рівні БД
        product_reviews = self._review_repo.get_by_product_and_rating(product_id, rating)
        
        response = {
            "product_id": product_id,
            "rating": rating,
            "count": product_reviews.count,
            "reviews": [
                {
                    "review_id": r.review_id,
                    "star_rating": r.star_rating,
                    "review_date": str(r.review_date)
                }
                for r in product_reviews.reviews
            ]
        }
        
        self._cache_repo.set(cache_key, response, self._cache_ttl)
        return response

class GetCustomerReviewsUseCase:
    """
    Сценарій використання: Перегляд активності клієнта.
    
    Цей сценарій фокусується на користувачеві, надаючи інформацію про всі
    товари, які він оцінив.
    """
    
    def __init__(
        self, 
        review_repo: ReviewRepository, 
        cache_repo: CacheRepository,
        cache_ttl: int = 60
    ):
        self._review_repo = review_repo
        self._cache_repo = cache_repo
        self._cache_ttl = cache_ttl
    
    def execute(self, customer_id: str) -> dict:
        """Повертає історію відгуків користувача з підтримкою кешування."""
        cache_key = f"customer:{customer_id}"
        
        cached = self._cache_repo.get(cache_key)
        if cached:
            return cached
        
        customer_reviews = self._review_repo.get_by_customer(customer_id)
        
        response = {
            "customer_id": customer_id,
            "count": customer_reviews.count,
            "reviews": [
                {
                    "review_id": r.review_id,
                    "product_id": r.product_id, # Тут важливо бачити, до яких товарів відгуки
                    "star_rating": r.star_rating
                }
                for r in customer_reviews.reviews
            ]
        }
        
        self._cache_repo.set(cache_key, response, self._cache_ttl)
        return response