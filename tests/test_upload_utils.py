from pathlib import Path

import pytest

import datacollective.upload_utils as upload_utils_module
from datacollective.models import UploadPart
from datacollective.upload_utils import (
    DEFAULT_PART_SIZE,
    MAX_UPLOAD_PARTS,
    UploadState,
    _complete_upload,
    _default_state_path,
    _get_presigned_part_url,
    _initiate_upload,
    _save_upload_state,
    _load_upload_state,
    _ensure_part_size_is_valid,
    _expected_parts,
    _state_matches,
)


def _build_state(**overrides: object) -> UploadState:
    values: dict[str, object] = {
        "submissionId": "submission",
        "fileUploadId": "file-upload",
        "uploadId": "upload-id",
        "fileSize": 1024,
        "partSize": DEFAULT_PART_SIZE,
        "filename": "dataset.tar.gz",
        "mimeType": "application/gzip",
    }
    values.update(overrides)
    return UploadState(**values)  # type: ignore[arg-type]


@pytest.fixture
def captured_requests(monkeypatch) -> list[dict[str, object]]:
    """Record the upload API calls instead of sending them."""
    requests: list[dict[str, object]] = []

    class FakeResponse:
        def json(self) -> dict[str, object]:
            return {
                "fileUploadId": "file-upload",
                "uploadId": "upload-id",
                "url": "https://storage.example.test/part",
                "partNumber": 1,
            }

    def fake_send_api_request(
        method: str, url: str, json_body: dict[str, object] | None = None
    ) -> FakeResponse:
        requests.append({"method": method, "url": url, "json_body": json_body})
        return FakeResponse()

    monkeypatch.setattr(
        upload_utils_module, "_get_api_url", lambda: "https://api.example.test"
    )
    monkeypatch.setattr(upload_utils_module, "_send_api_request", fake_send_api_request)
    return requests


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


def test_dataset_upload_uses_uploads_endpoints(
    captured_requests: list[dict[str, object]],
) -> None:
    _initiate_upload(
        "submission", "dataset.tar.gz", 1024, "application/gzip", DEFAULT_PART_SIZE
    )
    _get_presigned_part_url("file-upload", 1, "submission")
    _complete_upload(
        "file-upload",
        "upload-id",
        [UploadPart(partNumber=1, etag="etag-1")],
        "abc123",
        "submission",
    )

    assert [request["url"] for request in captured_requests] == [
        "https://api.example.test/uploads",
        "https://api.example.test/uploads/file-upload/parts/1",
        "https://api.example.test/uploads/file-upload",
    ]


def test_sample_upload_uses_submission_sample_endpoints(
    captured_requests: list[dict[str, object]],
) -> None:
    _initiate_upload(
        "submission",
        "sample.tar.gz",
        1024,
        "application/gzip",
        DEFAULT_PART_SIZE,
        is_sample=True,
    )
    _get_presigned_part_url("file-upload", 2, "submission", is_sample=True)
    _complete_upload(
        "file-upload",
        "upload-id",
        [UploadPart(partNumber=1, etag="etag-1")],
        "abc123",
        "submission",
        is_sample=True,
    )

    assert [(request["method"], request["url"]) for request in captured_requests] == [
        ("POST", "https://api.example.test/submissions/submission/sample"),
        (
            "GET",
            "https://api.example.test/submissions/submission/sample/file-upload/parts/2",
        ),
        ("POST", "https://api.example.test/submissions/submission/sample/file-upload"),
    ]


def test_sample_upload_initiate_payload_matches_dataset_upload(
    captured_requests: list[dict[str, object]],
) -> None:
    _initiate_upload(
        "submission",
        "sample.tar.gz",
        1024,
        "application/gzip",
        DEFAULT_PART_SIZE,
        is_sample=True,
    )

    assert captured_requests[0]["json_body"] == {
        "submissionId": "submission",
        "filename": "sample.tar.gz",
        "fileSize": 1024,
        "mimeType": "application/gzip",
    }


def test_sample_upload_uses_a_separate_default_state_file() -> None:
    archive = Path("/data/dataset.tar.gz")

    dataset_state_path = _default_state_path(archive)
    sample_state_path = _default_state_path(archive, is_sample=True)

    assert dataset_state_path.name == "dataset.tar.gz.mdc-upload.json"
    assert sample_state_path.name == "dataset.tar.gz.mdc-sample-upload.json"
    assert dataset_state_path != sample_state_path


def test_state_matches_rejects_state_for_the_other_upload_kind() -> None:
    sample_state = _build_state(isSample=True)

    assert _state_matches(
        sample_state, "submission", "dataset.tar.gz", 1024, is_sample=True
    )
    assert not _state_matches(sample_state, "submission", "dataset.tar.gz", 1024)


def test_upload_state_defaults_to_a_dataset_upload() -> None:
    # State files written before sample uploads existed must still load.
    assert _build_state().isSample is False
