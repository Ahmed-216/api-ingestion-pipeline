import pytest
from unittest.mock import Mock, patch
from api_ingestion_pipeline.utils.api_ingestion import ApiIngestion
from requests.exceptions import HTTPError, ConnectionError
import tempfile
import os
import shutil
import pandas as pd


# --------------------------------------------
# Fixtures
# --------------------------------------------


@pytest.fixture
def temp_dir():
    """
    Create a temporary directory for testing.

    This fixture creates a unique temporary directory that is automatically
    cleaned up after each test that uses it. This ensures test isolation
    and prevents leftover files from affecting other tests.

    Returns:
        str: Path to the created temporary directory
    """
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def base_ingestion(temp_dir):
    """Create a ApiIngestion instance with temp directory."""
    return ApiIngestion(output_dir=temp_dir)


# --------------------------------------------
# Core Functionality Tests
# --------------------------------------------


def test_initialization():
    """Test ApiIngestion initialization."""
    ingestion = ApiIngestion(output_dir="/tmp/test")
    assert ingestion.output_dir == "/tmp/test"
    assert ingestion.session is not None


def test_session_headers_initialization():
    """Test that session headers are properly initialized."""
    headers = {"User-Agent": "TestBot"}
    ingestion = ApiIngestion(output_dir="/tmp/test", headers=headers)
    assert ingestion.session.headers.get("User-Agent") == "TestBot"


# --------------------------------------------
# Error Handling Tests
# --------------------------------------------


@pytest.mark.parametrize(
    "num_errors", [1, 2, 3, 4]
)  # Test up to 4 retries (max_retries=5)
@pytest.mark.parametrize("error_type", ["rate_limit", "connection", "timeout"])
def test_error_recovery_success(base_ingestion, num_errors, error_type):
    """
    Test that the ApiIngestion keeps retrying when facing different error types until it succeeds.
    This test simulates scenarios where the API returns errors for a number of times defined by num_errors,
    and then returns a successful response.

    Checks:
        - The number of times the sleep function is called
        - The number of times the API is called
        - The final data returned by the API
    """
    from requests.exceptions import ReadTimeout

    # Create error based on type
    if error_type == "rate_limit":
        error_response = Mock(status_code=429)
        error_response.headers = {"Retry-After": "1"}
        error = HTTPError(response=error_response)
    elif error_type == "connection":
        error = ConnectionError("Network error")
    elif error_type == "timeout":
        error = ReadTimeout("Request timed out")

    success_response = Mock()
    success_response.json.return_value = {
        "data": [{"id": 1}],
        "pagination": {"page": 1, "total_pages": 1},
    }
    success_response.raise_for_status.return_value = None
    success_response.text = (
        '{"data": [{"id": 1}], "pagination": {"page": 1, "total_pages": 1}}'
    )

    # Create a list of side effects: num_errors followed by a success response
    side_effects = [error] * num_errors + [success_response]

    with (
        patch.object(
            base_ingestion.session, "get", side_effect=side_effects
        ) as mock_get,
        patch("time.sleep") as mock_sleep,
    ):
        data = base_ingestion.fetch_paginated_data(
            "dummy_url", data_key="data", per_page=1, total_pages_param="total_pages"
        )

        assert mock_sleep.call_count == num_errors  # Sleep called for each error
        assert (
            mock_get.call_count == num_errors + 1
        )  # API called for each error + 1 success
        assert data == [{"id": 1}]  # Data returned from the success response


@pytest.mark.parametrize("error_type", ["rate_limit", "connection", "timeout"])
def test_error_max_retries_exceeded(base_ingestion, error_type):
    """
    Test that the ApiIngestion raises an error when max retries are exceeded for different error types.
    """
    from requests.exceptions import ReadTimeout

    # Create error based on type
    if error_type == "rate_limit":
        error_response = Mock(status_code=429)
        error_response.headers = {"Retry-After": "1"}
        error = HTTPError(response=error_response)
    elif error_type == "connection":
        error = ConnectionError("Network error")
    elif error_type == "timeout":
        error = ReadTimeout("Request timed out")

    # Create a list of side effects: 5 errors (max_retries=5)
    side_effects = [error] * 5

    with (
        patch.object(
            base_ingestion.session, "get", side_effect=side_effects
        ) as mock_get,
        patch("time.sleep") as mock_sleep,
    ):
        # Check that an exception is raised when max retries are exceeded
        with pytest.raises(Exception, match="Max retries exceeded"):
            base_ingestion.fetch_paginated_data("dummy_url", data_key="data")

        assert mock_sleep.call_count == 5  # Sleep called for each error
        assert mock_get.call_count == 5  # API called for each error


def test_http_400_error_immediate_failure(base_ingestion):
    """
    Test that HTTP 400 errors cause immediate failure without retries.
    """
    error_response = Mock(status_code=400)
    http_400_error = HTTPError(response=error_response)

    with patch.object(
        base_ingestion.session, "get", side_effect=http_400_error
    ) as mock_get:
        with pytest.raises(HTTPError):
            base_ingestion.fetch_paginated_data("dummy_url", data_key="data")

        assert mock_get.call_count == 1  # Only one call, no retries


def test_json_decode_error_handling(base_ingestion):
    """
    Test handling of JSON decode errors.
    """
    invalid_json_response = Mock()
    invalid_json_response.json.side_effect = ValueError("Invalid JSON")
    invalid_json_response.raise_for_status.return_value = None
    invalid_json_response.text = "invalid json content"

    with patch.object(
        base_ingestion.session, "get", return_value=invalid_json_response
    ) as mock_get:
        with pytest.raises(ValueError, match="Invalid JSON"):
            base_ingestion.fetch_paginated_data("dummy_url", data_key="data")

        assert mock_get.call_count == 1  # Only one call, no retries


# --------------------------------------------
# CSV Saving Tests
# --------------------------------------------


def test_save_to_csv_creates_file(base_ingestion):
    """Test that save_to_csv creates the expected file."""
    test_data = [{"Country": "USA", "Value": 100}, {"Country": "FRA", "Value": 50}]

    filename = "test_output.csv"
    base_ingestion.save_to_csv(test_data, filename, base_ingestion.output_dir)

    expected_file_path = os.path.join(base_ingestion.output_dir, filename)
    assert os.path.exists(expected_file_path)

    # Verify file content
    df = pd.read_csv(expected_file_path)
    assert len(df) == 2
    assert "Country" in df.columns
    assert "Value" in df.columns


def test_save_to_csv_with_empty_data(base_ingestion):
    """Test save_to_csv with empty data."""
    empty_data = []
    filename = "empty.csv"

    base_ingestion.save_to_csv(empty_data, filename, base_ingestion.output_dir)

    expected_file_path = os.path.join(base_ingestion.output_dir, filename)
    assert os.path.exists(expected_file_path)

    # Verify empty file - check file size instead of reading
    assert os.path.getsize(expected_file_path) > 0  # File exists and has content


# --------------------------------------------
# Generic API Pagination Tests
# --------------------------------------------


@pytest.mark.parametrize(
    "scenario_name,pages_data,expected_count",
    [
        # Full 1 page
        (
            "full_1_page",
            [
                {
                    "data": [{"id": 1}, {"id": 2}, {"id": 3}],
                    "pagination": {"page": 1, "total_pages": 1},
                }
            ],
            3,
        ),
        # Full 3 pages
        (
            "full_3_pages",
            [
                {
                    "data": [{"id": 1}, {"id": 2}],
                    "pagination": {"page": 1, "total_pages": 3},
                },
                {
                    "data": [{"id": 3}, {"id": 4}],
                    "pagination": {"page": 2, "total_pages": 3},
                },
                {
                    "data": [{"id": 5}, {"id": 6}],
                    "pagination": {"page": 3, "total_pages": 3},
                },
            ],
            6,
        ),
        # Full 2 pages + partial third page
        (
            "full_2_pages_partial_third",
            [
                {
                    "data": [{"id": 1}, {"id": 2}],
                    "pagination": {"page": 1, "total_pages": 3},
                },
                {
                    "data": [{"id": 3}, {"id": 4}],
                    "pagination": {"page": 2, "total_pages": 3},
                },
                {
                    "data": [{"id": 5}],  # Partial page
                    "pagination": {"page": 3, "total_pages": 3},
                },
            ],
            5,
        ),
        # Full 2 pages + empty third page
        (
            "full_2_pages_empty_third",
            [
                {
                    "data": [{"id": 1}, {"id": 2}],
                    "pagination": {"page": 1, "total_pages": 3},
                },
                {
                    "data": [{"id": 3}, {"id": 4}],
                    "pagination": {"page": 2, "total_pages": 3},
                },
                {
                    "data": [],  # Empty last page
                    "pagination": {"page": 3, "total_pages": 3},
                },
            ],
            4,
        ),
    ],
)
def test_generic_api_pagination(
    scenario_name, pages_data, expected_count, base_ingestion
):
    """Test pagination logic for generic API responses."""
    # Create mock responses for each page
    mock_responses = []
    for page_data in pages_data:
        mock_response = Mock()
        mock_response.json.return_value = page_data
        mock_response.raise_for_status.return_value = None
        mock_response.text = str(page_data)
        mock_responses.append(mock_response)

    with patch.object(
        base_ingestion.session, "get", side_effect=mock_responses
    ) as mock_get:
        data = base_ingestion.fetch_paginated_data(
            "dummy_url", data_key="data", total_pages_param="total_pages", per_page=2
        )

        assert mock_get.call_count == len(pages_data), (
            f"Expected {len(pages_data)} API calls, got {mock_get.call_count} "
            f"for scenario: {scenario_name}",
        )

        assert len(data) == expected_count, (
            f"Expected {expected_count} records, got {len(data)} "
            f"for scenario: {scenario_name}"
        )


# --------------------------------------------
# End-to-end Tests
# --------------------------------------------


def test_ingest_and_save_integration(base_ingestion):
    """Test the complete ingest_and_save workflow."""
    mock_response = Mock()
    mock_response.json.return_value = [{"id": 1, "value": 100}]
    mock_response.raise_for_status.return_value = None
    mock_response.text = "[]"

    with patch.object(base_ingestion.session, "get", return_value=mock_response):
        base_ingestion.ingest_and_save(
            base_url="dummy_url",
            filename="test_integration.csv",
            output_dir=base_ingestion.output_dir,
        )

        expected_file_path = os.path.join(
            base_ingestion.output_dir, "test_integration.csv"
        )
        assert os.path.exists(expected_file_path)

        # Verify file content
        df = pd.read_csv(expected_file_path)
        assert len(df) == 1
        assert "id" in df.columns
        assert "value" in df.columns
