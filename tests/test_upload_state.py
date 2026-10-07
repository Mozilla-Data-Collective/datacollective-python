import pytest
from pathlib import Path

import datacollective.upload as upload_module
from datacollective.errors import ResourceRemovedError
from datacollective.upload import (
    upload_dataset_file,
    upload_sample_file,
)


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
