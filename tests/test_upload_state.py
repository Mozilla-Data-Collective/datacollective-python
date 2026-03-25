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
        ValueError,
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
