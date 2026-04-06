from src.infrastructure.repositories.memory_repository import MemoryReviewRepository
from src.application.services.review_service import ReviewService


#assembling domain, application, infrastructure

def run_test():
    print("--- Запуск тесту архітектури ---")

    repo = MemoryReviewRepository()
    service = ReviewService(repository=repo)

    print("\nСтворюємо новий відгук...")
    new_review = service.create_review(
        product_id="prod-100",
        customer_id="user-42",
        rating=5,
        body="Це найкращий курс з Data Engineering!"
    )

    print(f"\nШукаємо відгук з ID: {new_review.review_id}")
    found_review = repo.get_by_id(new_review.review_id)

    if found_review:
        print(f"Успіх! Знайдено відгук: {found_review.review_body}")
        print(f"Дата створення: {found_review.review_date}")
    else:
        print("Помилка: відгук не знайдено.")

if __name__ == "__main__":
    run_test()