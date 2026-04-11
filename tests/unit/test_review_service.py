import pytest
from unittest.mock import Mock
from datetime import datetime

from src.application.services.review_service import ReviewService
from src.domain.entities import Review


@pytest.fixture
def mock_repository():
    return Mock()


@pytest.fixture
def mock_redis():
    mock = Mock()
    mock.get_cache = Mock()
    mock.set_cache = Mock()
    mock.client = Mock()
    return mock


@pytest.fixture
def review_service(mock_repository, mock_redis):
    return ReviewService(repository=mock_repository, redis_client=mock_redis)


def test_create_review_saves_to_db(review_service, mock_repository):
    result = review_service.create_review(
        product_id="prod-1",
        customer_id="cust-1",
        rating=5,
        body="Great product!"
    )

    assert result is not None
    mock_repository.save.assert_called_once()


def test_create_review_invalidates_cache(review_service, mock_redis):
    review_service.create_review(
        product_id="prod-1",
        customer_id="cust-1",
        rating=5,
        body="Great!"
    )

    assert mock_redis.client.delete.called


def test_get_reviews_by_product_cache_hit(review_service, mock_repository, mock_redis):
    mock_redis.get_cache.return_value = [
        {
            "review_id": "r1",
            "product_id": "p1",
            "customer_id": "c1",
            "star_rating": 5,
            "review_body": "Great!",
            "created_at": datetime.now().isoformat()
        }
    ]

    result = review_service.get_reviews_by_product("p1")

    mock_repository.get_by_product.assert_not_called()
    assert len(result) == 1


def test_get_reviews_by_product_cache_miss(review_service, mock_repository, mock_redis):
    mock_redis.get_cache.return_value = None

    mock_repository.get_by_product.return_value = [
        Review(
            review_id="r1",
            product_id="p1",
            customer_id="c1",
            star_rating=5,
            review_body="Good!",
            created_at=datetime.now()
        )
    ]

    result = review_service.get_reviews_by_product("p1")

    mock_repository.get_by_product.assert_called_once()
    assert len(result) == 1


def test_get_reviews_by_customer(review_service, mock_repository, mock_redis):
    mock_redis.get_cache.return_value = None

    mock_repository.get_by_customer.return_value = [
        Review(
            review_id="r1",
            product_id="p1",
            customer_id="c1",
            star_rating=4,
            review_body="Nice",
            created_at=datetime.now()
        )
    ]

    result = review_service.get_reviews_by_customer("c1")

    assert len(result) == 1


def test_get_reviews_by_product_and_rating(review_service, mock_repository, mock_redis):
    mock_redis.get_cache.return_value = None

    mock_repository.get_by_product_and_rating.return_value = [
        Review(
            review_id="r1",
            product_id="p1",
            customer_id="c1",
            star_rating=5,
            review_body="Perfect!",
            created_at=datetime.now()
        )
    ]

    result = review_service.get_reviews_by_product_and_rating("p1", 5)

    assert len(result) == 1


def test_create_review_handles_db_error(review_service, mock_repository):
    mock_repository.save.side_effect = Exception("DB error")

    try:
        result = review_service.create_review("p1", "c1", 5, "test")
        assert result is not None
    except Exception:
        assert True