from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd

from datacollective.datasets import (
    _strip_archive_suffix,
    get_dataset_details,
    load_dataset,

)
from datacollective.schema import DatasetSchema

BASE_API_URL = "https://datacollective.mozillafoundation.org/api/datasets"


@pytest.fixture
def mock_pipeline():
    """
    A pytest fixture that mocks all the external dependencies and side-effects
    called within `load_dataset`. This prevents real network requests and file I/O.
    """
    with (
        patch("datacollective.datasets.get_dataset_details") as mock_details,
        patch("datacollective.datasets.get_dataset_schema") as mock_schema_get,
        patch("datacollective.datasets.get_download_plan") as mock_plan,
        patch("datacollective.datasets.save_dataset_to_disk") as mock_save,
        patch("datacollective.datasets.resolve_download_dir") as mock_resolve,
        patch("datacollective.datasets._extract_archive") as mock_extract,
        patch("datacollective.datasets._resolve_schema") as mock_schema_resolve,
        patch("datacollective.datasets.load_dataset_from_schema") as mock_load,
    ):
        # Mock API details and schema retrieval
        mock_details.return_value = {"id": "test_id_123"}
        mock_schema_get.return_value = DatasetSchema(dataset_id="test_id_123", task="ASR")

        # Mock the download plan and checksum
        mock_plan_obj = MagicMock()
        mock_plan_obj.checksum = "fake_checksum_abc"
        mock_plan.return_value = mock_plan_obj

        # Mock file paths and directories
        mock_save.return_value = Path("/fake/downloads/archive.zip")
        mock_resolve.return_value = Path("/fake/downloads")
        mock_extract.return_value = Path("/fake/downloads/extracted_data")

        # Mock the final schema resolution and DataFrame loading
        mock_schema_resolve.return_value = DatasetSchema(dataset_id="test_id_123", task="ASR")
        expected_df = pd.DataFrame({"colA": [1, 2], "colB": ["x", "y"]})
        mock_load.return_value = expected_df

        # Yield all mocks as a dictionary so the test can assert against them
        yield {
            "details": mock_details,
            "schema_get": mock_schema_get,
            "plan": mock_plan,
            "save": mock_save,
            "resolve": mock_resolve,
            "extract": mock_extract,
            "schema_resolve": mock_schema_resolve,
            "load": mock_load,
            "expected_df": expected_df,
        }


def test_strip_archive_suffix_removes_known_extensions(tmp_path: Path) -> None:
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


def test_load_dataset_happy_path(mock_pipeline):
    """
    Test that `load_dataset` correctly orchestrates the fetching, downloading,
    extracting, and loading of a dataset when all dependencies succeed.
    """
    # Arrange
    dataset_slug = "my-awesome-dataset"
    mocks = mock_pipeline

    # Act
    result_df = load_dataset(
        dataset_id=dataset_slug, show_progress=False, overwrite_existing=False
    )

    # Assert
    # Check that the final output is the expected DataFrame
    pd.testing.assert_frame_equal(result_df, mocks["expected_df"])

    # Verify the orchestrator called the dependencies in the correct order with correct arguments
    mocks["details"].assert_called_once_with(dataset_slug)
    mocks["schema_get"].assert_called_once_with("test_id_123")

    mocks["save"].assert_called_once_with(
        dataset_id="test_id_123",
        download_directory=None,
        show_progress=False,
        overwrite_existing=False,
    )

    mocks["extract"].assert_called_once_with(
        archive_path=Path("/fake/downloads/archive.zip"),
        dest_dir=Path("/fake/downloads"),
        overwrite_extracted=False,
    )

    # Ensure the final load was triggered with the resolved schema and extracted directory
    mocks["load"].assert_called_once_with(
        DatasetSchema(dataset_id="test_id_123", task="ASR"), Path("/fake/downloads/extracted_data")
    )
