import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from src.domain.entities import Review
from src.infrastructure.repositories.cassandra_repository import CassandraReviewRepository

@pytest.fixture
def repo():
    """
    Creates CassandraReviewRepository with fully mocked Cassandra session.
    No real DB connection is made.
    """

    with patch("src.infrastructure.repositories.cassandra_repository.Cluster") as mock_cluster:

        mock_session = MagicMock()

        # prevent real connection
        mock_cluster.return_value.connect.return_value = mock_session

        # avoid real prepare calls
        mock_session.prepare.return_value = "prepared_stmt"

        repo = CassandraReviewRepository()

        # replace prepared statements with mocks
        repo.session = mock_session
        repo.insert_by_product = "insert1"
        repo.insert_by_product_rating = "insert2"
        repo.insert_by_customer = "insert3"
        repo.select_by_product = "select1"
        repo.select_by_product_rating = "select2"
        repo.select_by_customer = "select3"

        return repo, mock_session

def test_save_writes_to_three_tables(repo):
    repo_obj, session = repo

    review = MagicMock(spec=Review)
    review.review_id = "r1"
    review.product_id = "p1"
    review.customer_id = "123"
    review.star_rating = 5
    review.review_body = "Excellent"
    review.created_at = datetime.utcnow()

    result = repo_obj.save(review)

    assert result is True
    assert session.execute.call_count == 3

def test_get_by_product(repo):
    repo_obj, session = repo

    mock_row = MagicMock()
    mock_row.review_id = "r1"
    mock_row.product_id = "p1"
    mock_row.customer_id = 123
    mock_row.star_rating = 5
    mock_row.review_body = "Good"
    mock_row.created_at = datetime.utcnow()

    session.execute.return_value = [mock_row]

    result = repo_obj.get_by_product("p1")

    assert len(result) == 1
    assert result[0].product_id == "p1"

def test_get_by_customer_converts_id(repo):
    repo_obj, session = repo

    session.execute.return_value = []

    repo_obj.get_by_customer("123")

    args, _ = session.execute.call_args

    # ensure string -> int conversion
    assert args[1] == [123]

def test_save_handles_exception(repo):
    repo_obj, session = repo

    session.execute.side_effect = Exception("DB failure")

    review = MagicMock(spec=Review)
    review.review_id = "r1"
    review.product_id = "p1"
    review.customer_id = "123"
    review.star_rating = 5
    review.review_body = "Bad"
    review.created_at = datetime.utcnow()

    result = repo_obj.save(review)

    assert result is False

def test_row_to_entity():
    row = MagicMock()
    row.review_id = "r1"
    row.product_id = "p1"
    row.customer_id = 123
    row.star_rating = 4
    row.review_body = "Nice"
    row.created_at = datetime.utcnow()

    entity = CassandraReviewRepository._row_to_entity(row)

    assert entity.review_id == "r1"
    assert entity.product_id == "p1"
    assert entity.customer_id == 123
    assert entity.star_rating == 4

