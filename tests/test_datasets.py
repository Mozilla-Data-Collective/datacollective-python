from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

from datacollective.datasets import (
    _strip_archive_suffix,
    get_dataset_details,
)

BASE_API_URL = "https://datacollective.mozillafoundation.org/api/datasets"


def test_strip_archive_suffix_removes_known_extensions(tmp_path: Path) -> None:
    """Test strip compress file suffix of known extensions"""
    tar_path = tmp_path / "sample.tar.gz"
    zip_path = tmp_path / "sample.zip"

    assert _strip_archive_suffix(tar_path).name == "sample"
    assert _strip_archive_suffix(zip_path).name == "sample"


def test_get_dataset_details_success() -> None:
    """Test successful retrieval of dataset details."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": "test123", "name": "Test Dataset"}

    with patch(
        "datacollective.datasets.send_api_request", return_value=mock_response
    ) as mock_send:
        result = get_dataset_details("test123")

        mock_send.assert_called_once_with(
            method="GET",
            url=f"{BASE_API_URL}/test123",  # Assuming this is the base URL
        )
        assert result == {"id": "test123", "name": "Test Dataset"}


def test_get_dataset_details_empty_id() -> None:
    """Test that empty dataset_id raises ValueError."""
    with pytest.raises(ValueError, match="`dataset_id` must be a non-empty string"):
        get_dataset_details("")