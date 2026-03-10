import pytest

from pathlib import Path

from datacollective.models import UploadPart
from datacollective.upload import (
    UploadState,
    load_upload_state,
    save_upload_state,
    upload_dataset_file,
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

    save_upload_state(state_path, state)
    loaded = load_upload_state(state_path)

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

    assert load_upload_state(state_path) is None


def test_upload_dataset_file_rejects_missing_file(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.tar.gz"

    with pytest.raises(FileNotFoundError, match="File not found"):
        upload_dataset_file(str(missing_file), submission_id="submission")


def test_upload_dataset_file_rejects_empty_file(tmp_path: Path) -> None:
    empty_file = tmp_path / "empty.tar.gz"
    empty_file.write_bytes(b"")

    with pytest.raises(ValueError, match="non-empty file"):
        upload_dataset_file(str(empty_file), submission_id="submission")
