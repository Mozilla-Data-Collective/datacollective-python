import pytest
from pathlib import Path

import datacollective.upload as upload_module
import datacollective.upload_utils as upload_utils_module
from datacollective.errors import ResourceRemovedError
from datacollective.upload import (
    upload_dataset_file,
    upload_sample_file,
)
from datacollective.upload_utils import UploadState, _save_upload_state


def test_upload_dataset_file_rejects_missing_file(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.tar.gz"

    with pytest.raises(FileNotFoundError, match="File not found"):
        upload_dataset_file(str(missing_file), submission_id="submission")


def test_upload_dataset_file_rejects_empty_file(tmp_path: Path) -> None:
    empty_file = tmp_path / "empty.tar.gz"
    empty_file.write_bytes(bytearray())

    with pytest.raises(ValueError, match="non-empty file"):
        upload_dataset_file(str(empty_file), submission_id="submission")


def test_upload_sample_file_rejects_missing_file(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing-sample.tar.gz"

    with pytest.raises(FileNotFoundError, match="File not found"):
        upload_sample_file(str(missing_file), submission_id="submission")


def test_upload_sample_file_rejects_empty_file(tmp_path: Path) -> None:
    empty_file = tmp_path / "empty-sample.tar.gz"
    empty_file.write_bytes(bytearray())

    with pytest.raises(ValueError, match="non-empty file"):
        upload_sample_file(str(empty_file), submission_id="submission")


def test_upload_removes_state_file_when_submission_is_deleted(
    tmp_path: Path, monkeypatch
) -> None:
    archive = tmp_path / "dataset.tar.gz"
    archive.write_bytes(b"data")
    state_file = tmp_path / "upload.state.json"

    def fake_load_or_create_state(**_):
        state_file.write_text("{}")
        raise ResourceRemovedError("Submission deleted")

    monkeypatch.setattr(
        upload_module, "_load_or_create_state", fake_load_or_create_state
    )

    with pytest.raises(ResourceRemovedError):
        upload_dataset_file(
            file_path=str(archive), submission_id="c123", state_path=str(state_file)
        )

    assert not state_file.exists()


def test_upload_removes_state_file_when_worker_reports_submission_deleted(
    tmp_path: Path, monkeypatch
) -> None:
    """A 410 raised on a worker thread must still reach `_upload_file`."""
    archive = tmp_path / "dataset.tar.gz"
    archive.write_bytes(b"data")
    state_file = tmp_path / "upload.state.json"

    def fake_load_or_create_state(**_) -> UploadState:
        state = UploadState(
            submissionId="c123",
            fileUploadId="file-upload",
            uploadId="upload-id",
            fileSize=4,
            partSize=2,
            filename="dataset.tar.gz",
            mimeType="application/gzip",
        )
        _save_upload_state(state_file, state)
        return state

    def fake_presign(*_, **__):
        raise ResourceRemovedError("Submission deleted")

    monkeypatch.setattr(
        upload_module, "_load_or_create_state", fake_load_or_create_state
    )
    monkeypatch.setattr(upload_utils_module, "_get_presigned_part_url", fake_presign)

    with pytest.raises(ResourceRemovedError):
        upload_dataset_file(
            file_path=str(archive),
            submission_id="c123",
            state_path=str(state_file),
            show_progress=False,
            max_workers=2,
        )

    assert not state_file.exists()


def test_upload_dataset_file_rejects_invalid_max_workers(
    tmp_path: Path, monkeypatch
) -> None:
    archive = tmp_path / "dataset.tar.gz"
    archive.write_bytes(b"data")

    def unexpected_network_call(**_):
        raise AssertionError("no network call expected")

    monkeypatch.setattr(upload_module, "_load_or_create_state", unexpected_network_call)

    with pytest.raises(ValueError, match="max_workers"):
        upload_dataset_file(str(archive), submission_id="submission", max_workers=0)
