from pathlib import Path

import datacollective.submissions as submissions_module
import pytest

from datacollective.models import DatasetSubmission, License, Task


def _build_complete_submission(
    *, file_upload_id: str | None = "file-upload-id"
) -> DatasetSubmission:
    values: dict[str, object] = {
        "name": "Dataset Name",
        "longDescription": "A detailed description of the dataset.",
        "task": Task.ASR,
        "locale": "en-US",
        "format": "TSV",
        "licenseAbbreviation": License.CC_BY_4_0,
        "restrictions": "No restrictions.",
        "forbiddenUsage": "Do not use for unlawful purposes.",
        "pointOfContactFullName": "Jane Doe",
        "pointOfContactEmail": "jane@example.com",
        "agreeToSubmit": True,
    }
    if file_upload_id is not None:
        values["fileUploadId"] = file_upload_id
    return DatasetSubmission(**values)


def test_submit_submission_allows_minimal_payload_for_existing_remote_draft(
    monkeypatch,
) -> None:
    captured_request: dict[str, object] = {}

    class FakeResponse:
        def json(self) -> dict[str, object]:
            return {"submission": {"id": "submission-id", "status": "submitted"}}

    def fake_send_api_request(
        method: str, url: str, json_body: dict[str, object] | None = None
    ) -> FakeResponse:
        captured_request.update(
            {"method": method, "url": url, "json_body": json_body or {}}
        )
        return FakeResponse()

    monkeypatch.setattr(
        submissions_module, "_get_api_url", lambda: "https://api.example.test"
    )
    monkeypatch.setattr(submissions_module, "send_api_request", fake_send_api_request)

    response = submissions_module.submit_submission(
        "submission-id",
        DatasetSubmission(agreeToSubmit=True),
    )

    assert response == {"submission": {"id": "submission-id", "status": "submitted"}}
    assert captured_request == {
        "method": "POST",
        "url": "https://api.example.test/submissions/submission-id",
        "json_body": {"agreeToSubmit": True},
    }


def test_submit_submission_requires_file_upload_id_for_local_final_submission(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        submissions_module,
        "send_api_request",
        lambda *args, **kwargs: pytest.fail(
            "submit_submission should fail validation before calling the API"
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        submissions_module.submit_submission(
            "submission-id",
            _build_complete_submission(file_upload_id=None),
        )

    assert str(exc_info.value) == (
        "Cannot submit dataset. Missing required fields for final submission: "
        "`fileUploadId`. Upload the dataset file first to get a `fileUploadId`."
    )


def test_create_submission_with_upload_rejects_missing_required_metadata_before_upload(
    tmp_path: Path, monkeypatch
) -> None:
    archive_path = tmp_path / "dataset.tar.gz"
    archive_path.write_bytes(bytearray(b"dataset-payload"))

    monkeypatch.setattr(
        submissions_module,
        "create_submission_draft",
        lambda submission: pytest.fail(
            "create_submission_with_upload should fail before creating a draft"
        ),
    )
    monkeypatch.setattr(
        submissions_module,
        "upload_dataset_file",
        lambda *args, **kwargs: pytest.fail(
            "create_submission_with_upload should fail before uploading"
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        submissions_module.create_submission_with_upload(
            file_path=str(archive_path),
            submission=DatasetSubmission(name="Dataset Name", agreeToSubmit=True),
            verbose=False,
        )

    message = str(exc_info.value)
    assert "Missing required fields for final submission" in message
    assert "`longDescription`" in message
    assert "either `licenseAbbreviation` or `license`" in message
    assert "`fileUploadId`" not in message
