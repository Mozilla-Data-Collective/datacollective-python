from pathlib import Path

import pytest

from datacollective.models import UploadPart
from datacollective.upload_utils import (
    DEFAULT_PART_SIZE,
    MAX_UPLOAD_PARTS,
    UploadState,
    _save_upload_state,
    _load_upload_state,
    _ensure_part_size_is_valid,
    _expected_parts,
)


def test_upload_state_round_trip(tmp_path: Path) -> None:
    state_path = tmp_path / "upload-state.json"
    state = UploadState(
        submissionId="submission",
        fileUploadId="file-upload",
        uploadId="upload-id",
        fileSize=1024,
        partSize=256,
        filename="dataset.tar.gz",
        mimeType="application/gzip",
        parts=[UploadPart(partNumber=1, etag="etag-1")],
        checksum="abc123",
    )

    _save_upload_state(state_path, state)
    loaded = _load_upload_state(state_path)

    assert loaded is not None
    assert loaded.fileUploadId == state.fileUploadId
    assert loaded.parts[0].partNumber == 1
    assert loaded.parts[0].etag == "etag-1"


def test_load_upload_state_returns_none_for_invalid_payload(tmp_path: Path) -> None:
    state_path = tmp_path / "upload-state.json"
    state_path.write_text(
        """{
  \"submissionId\": \"submission\",
  \"fileUploadId\": \"file-upload\",
  \"uploadId\": \"\",
  \"fileSize\": 1024,
  \"partSize\": 256,
  \"filename\": \"dataset.tar.gz\",
  \"mimeType\": \"application/gzip\",
  \"parts\": []
}"""
    )

    assert _load_upload_state(state_path) is None


def test_validate_part_count_rejects_too_many_parts() -> None:
    file_size = DEFAULT_PART_SIZE * (MAX_UPLOAD_PARTS + 1)
    with pytest.raises(ValueError, match="exceeding the limit"):
        _ensure_part_size_is_valid(file_size, DEFAULT_PART_SIZE)


def test_validate_part_count_allows_fitting_file() -> None:
    file_size = DEFAULT_PART_SIZE * MAX_UPLOAD_PARTS
    _ensure_part_size_is_valid(file_size, DEFAULT_PART_SIZE)


def test_validate_part_count_rejects_non_positive_part_size() -> None:
    with pytest.raises(ValueError, match="must be at least"):
        _ensure_part_size_is_valid(1024, 0)


def test_expected_parts_rounds_up_for_remainder() -> None:
    # A trailing partial chunk must get its own part.
    assert _expected_parts(file_size=250, part_size=100) == 3
    assert _expected_parts(file_size=200, part_size=100) == 2
