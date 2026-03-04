from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd

from datacollective.datasets import (
    _strip_archive_suffix,
    get_dataset_details,
    load_dataset,
    save_dataset_to_disk,
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


@pytest.fixture
def mock_download_plan():
    """
    Creates a mock download plan with simulated Path objects to avoid real file I/O.
    """
    plan = MagicMock()
    plan.checksum = "mock_checksum_hash"
    
    # Mock the internal Path objects
    plan.target_filepath = MagicMock(spec=Path)
    plan.tmp_filepath = MagicMock(spec=Path)
    plan.checksum_filepath = MagicMock(spec=Path)
    
    # By default, pretend the files don't exist
    plan.target_filepath.exists.return_value = False
    plan.checksum_filepath.exists.return_value = False
    
    return plan

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


@patch("datacollective.datasets.get_dataset_details")
@patch("datacollective.datasets.get_download_plan")
@patch("datacollective.datasets.determine_resume_state")
@patch("datacollective.datasets.write_checksum_file")
@patch("datacollective.datasets.execute_download_plan")
class TestSaveDatasetToDisk:

    def test_new_download_happy_path(
        self, 
        mock_execute, 
        mock_write_checksum, 
        mock_resume, 
        mock_get_plan, 
        mock_details, 
        mock_download_plan
    ):
        """Test the standard download flow when the file does not exist."""
        # Arrange
        mock_details.return_value = {"id": "internal_id_123"}
        mock_get_plan.return_value = mock_download_plan
        mock_resume.return_value = False
        mock_download_plan.checksum_filepath.exists.return_value = True # Pretend it was created

        # Act
        result = save_dataset_to_disk("my-dataset", show_progress=False)

        # Assert
        mock_details.assert_called_once_with("my-dataset")
        mock_get_plan.assert_called_once_with("internal_id_123", None)
        mock_resume.assert_called_once_with(mock_download_plan)
        
        # Verify it writes the checksum before downloading
        mock_write_checksum.assert_called_once_with(
            mock_download_plan.checksum_filepath, 
            "mock_checksum_hash"
        )
        mock_execute.assert_called_once_with(mock_download_plan, False, False)
        
        # Verify file operations at the end of a successful download
        mock_download_plan.tmp_filepath.replace.assert_called_once_with(mock_download_plan.target_filepath)
        mock_download_plan.checksum_filepath.unlink.assert_called_once()
        
        assert result == Path(mock_download_plan.target_filepath)

    def test_skip_download_if_exists(
        self, 
        mock_execute, 
        mock_write_checksum, 
        mock_resume, 
        mock_get_plan, 
        mock_details, 
        mock_download_plan
    ):
        """Test that the download is skipped if the target file already exists."""
        # Arrange
        mock_details.return_value = {"id": "internal_id_123"}
        mock_get_plan.return_value = mock_download_plan
        mock_download_plan.target_filepath.exists.return_value = True  # File exists!

        # Act
        result = save_dataset_to_disk("my-dataset", overwrite_existing=False)

        # Assert
        # The download shouldn't be executed
        mock_execute.assert_not_called()
        mock_write_checksum.assert_not_called()
        mock_download_plan.tmp_filepath.replace.assert_not_called()
        
        assert result == Path(mock_download_plan.target_filepath)

    @patch("datacollective.datasets.cleanup_partial_download")
    def test_overwrite_existing_file(
        self, 
        mock_cleanup,
        mock_execute, 
        mock_write_checksum, 
        mock_resume, 
        mock_get_plan, 
        mock_details, 
        mock_download_plan
    ):
        """Test that existing files are cleaned up before downloading if overwrite_existing=True."""
        # Arrange
        mock_details.return_value = {"id": "internal_id_123"}
        mock_get_plan.return_value = mock_download_plan
        mock_download_plan.target_filepath.exists.return_value = True  # File exists!
        mock_resume.return_value = False

        # Act
        save_dataset_to_disk("my-dataset", overwrite_existing=True)

        # Assert
        mock_cleanup.assert_called_once_with(
            mock_download_plan.tmp_filepath, 
            mock_download_plan.checksum_filepath
        )
        # Verify the old target file is deleted before the new download starts
        mock_download_plan.target_filepath.unlink.assert_called_once()
        mock_execute.assert_called_once()

    def test_dataset_not_found_raises_error(
        self, 
        mock_execute, 
        mock_write_checksum, 
        mock_resume, 
        mock_get_plan, 
        mock_details,
    ):
        """Test that a 404 from the API is gracefully wrapped in a RuntimeError."""
        # Arrange
        mock_details.side_effect = FileNotFoundError("Not found API response")

        # Act & Assert
        with pytest.raises(RuntimeError) as exc_info:
            save_dataset_to_disk("invalid-dataset")

        assert "does not exist in MDC or the ID is mistyped" in str(exc_info.value)
        mock_get_plan.assert_not_called()
        mock_execute.assert_not_called()