import datacollective.upload as upload_module
import pytest
from _pytest.monkeypatch import MonkeyPatch

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
  \"submissionId\": \"id\",
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
        upload_dataset_file(str(missing_file), dataset_id_or_slug="id")


def test_upload_dataset_file_rejects_empty_file(tmp_path: Path) -> None:
    empty_file = tmp_path / "empty.tar.gz"
    empty_file.write_bytes(bytearray())

    with pytest.raises(ValueError, match="non-empty file"):
        upload_dataset_file(str(empty_file), dataset_id_or_slug="id")


def test_resolve_submission_id_for_upload_requires_submission_id_from_dataset_details(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        upload_module,
        "get_dataset_details",
        lambda dataset_id: {"id": dataset_id},
    )

    with pytest.raises(
        RuntimeError,
        match="Dataset details did not return a `submission_id`",
    ):
        upload_module._resolve_submission_id(
            dataset_id_or_slug="sample-dataset-slug",
        )


def test_initiate_upload_posts_submission_id_without_stdout(
    monkeypatch: MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured_request: dict[str, object] = {}

    class FakeResponse:
        def json(self) -> dict[str, object]:
            return {
                "fileUploadId": "file-upload-id",
                "uploadId": "upload-id",
                "partSize": 1024,
            }

    def fake_send_api_request(
        method: str, url: str, json_body: dict[str, object] | None = None
    ) -> FakeResponse:
        captured_request.update(
            {"method": method, "url": url, "json_body": json_body or {}}
        )
        return FakeResponse()

    monkeypatch.setattr(
        upload_module, "_get_api_url", lambda: "https://api.example.test"
    )
    monkeypatch.setattr(upload_module, "send_api_request", fake_send_api_request)

    session = upload_module._initiate_upload(
        submission_id="cmmns07oe001onn07aeidopab",
        filename="dataset.tar.gz",
        file_size=123,
        mime_type="application/gzip",
    )
    output = capsys.readouterr()

    assert session.fileUploadId == "file-upload-id"
    assert session.uploadId == "upload-id"
    assert session.partSize == 1024
    assert captured_request == {
        "method": "POST",
        "url": "https://api.example.test/uploads",
        "json_body": {
            "submissionId": "cmmns07oe001onn07aeidopab",
            "filename": "dataset.tar.gz",
            "fileSize": 123,
            "mimeType": "application/gzip",
        },
    }
    assert output.out == ""


class _FakeUploadPartResponse:
    headers = {"ETag": '"etag-1"'}


def test_upload_dataset_file_uses_existing_submission_id_for_version_upload(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    archive_path = tmp_path / "dataset.tar.gz"
    archive_path.write_bytes(bytearray(b"versioned-payload"))
    captured_completion: dict[str, object] = {}

    def fake_load_or_create_state(
        state_file: Path,
        submission_id: str,
        final_filename: str,
        file_size: int,
    ) -> UploadState:
        assert state_file == archive_path.with_name("dataset.tar.gz.mdc-upload.json")
        assert submission_id == "cmmns07oe001onn07aeidopab"
        assert final_filename == "dataset.tar.gz"
        assert file_size == len(b"versioned-payload")
        return UploadState(
            submissionId=submission_id,
            fileUploadId="file-upload-id",
            uploadId="upload-id",
            fileSize=file_size,
            partSize=file_size,
            filename=final_filename,
            mimeType="application/gzip",
            parts=[],
            checksum=None,
        )

    def fake_complete_upload(
        file_upload_id: str,
        upload_id: str | None,
        parts: list[upload_module.UploadPart],
        checksum: str,
    ) -> dict[str, object]:
        captured_completion.update(
            {
                "fileUploadId": file_upload_id,
                "uploadId": upload_id,
                "parts": parts,
                "checksum": checksum,
            }
        )
        return {"ok": True}

    monkeypatch.setattr(
        upload_module, "_load_or_create_state", fake_load_or_create_state
    )
    monkeypatch.setattr(
        upload_module,
        "_get_presigned_part_url",
        lambda file_upload_id, part_number: upload_module.PresignedPartUrl(
            partNumber=part_number,
            url=f"https://upload.example.test/{file_upload_id}/{part_number}",
        ),
    )
    monkeypatch.setattr(
        upload_module,
        "_upload_part_with_retry",
        lambda presigned_url, payload: _FakeUploadPartResponse(),
    )
    monkeypatch.setattr(upload_module, "_complete_upload", fake_complete_upload)

    result = upload_dataset_file(
        file_path=str(archive_path),
        dataset_id_or_slug="cmmns07oe001onn07aeidopab",
        show_progress=False,
    )

    assert result.submissionId == "cmmns07oe001onn07aeidopab"
    assert result.fileUploadId == "file-upload-id"
    assert len(result.parts) == 1
    assert result.parts[0].partNumber == 1
    assert result.parts[0].etag == "etag-1"
    assert captured_completion["fileUploadId"] == "file-upload-id"
    assert captured_completion["uploadId"] == "upload-id"
    assert not archive_path.with_name("dataset.tar.gz.mdc-upload.json").exists()
