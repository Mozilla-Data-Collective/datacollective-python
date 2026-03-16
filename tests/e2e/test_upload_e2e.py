from datetime import datetime
from pathlib import Path

from datacollective.submissions import create_submission_with_upload
from datacollective.upload import upload_dataset_file
from tests.e2e.conftest import sample_dataset_submission, skip_if_rate_limited


def test_create_submission_with_upload(
    tmp_path: Path,
    live_api_env: None,
    example_dataset_archive_path: Path,
) -> None:
    name = f"python-sdk-e2e-{datetime.now().strftime('%H:%M - %d/%m/%Y')}"
    state_path = tmp_path / "predefined-license-upload-state.json"
    submission = sample_dataset_submission(name=name)

    try:
        response = create_submission_with_upload(
            file_path=str(example_dataset_archive_path),
            submission=submission,
            state_path=str(state_path),
            verbose=True,
        )
    except Exception as exc:  # noqa: BLE001
        skip_if_rate_limited(exc)
    else:
        submission_payload = response.get("submission", {})

        assert isinstance(response, dict)
        assert isinstance(submission_payload, dict)
        assert submission_payload.get("id")
        assert submission_payload.get("fileUploadId")
        assert not state_path.exists(), (
            "Upload state should be cleaned up after success"
        )


def test_upload_dataset_file_updates_approved_dataset_version(
    tmp_path: Path,
    live_api_env: None,
    example_dataset_archive_path: Path,
    approved_dataset_submission_id: str,
) -> None:
    state_path = tmp_path / "approved-dataset-version-upload-state.json"
    upload_state = None

    try:
        upload_state = upload_dataset_file(
            file_path=str(example_dataset_archive_path),
            submission_id=approved_dataset_submission_id,
            state_path=str(state_path),
            show_progress=False,
        )
    except Exception as exc:  # noqa: BLE001
        skip_if_rate_limited(exc)

    assert upload_state is not None
    assert upload_state.submissionId == approved_dataset_submission_id
    assert upload_state.fileUploadId
    assert upload_state.filename == example_dataset_archive_path.name
    assert upload_state.parts
    assert upload_state.checksum
    assert not state_path.exists(), "Upload state should be cleaned up after success"
