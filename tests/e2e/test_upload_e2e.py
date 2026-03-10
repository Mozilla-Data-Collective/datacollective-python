from datetime import datetime
from pathlib import Path

from datacollective.submissions import create_submission_with_upload
from tests.e2e.helpers import build_full_submission, skip_if_rate_limited


def test_create_submission_with_upload(
    tmp_path: Path,
    live_api_env: None,
    example_dataset_archive_path: Path,
) -> None:
    name = f"python-sdk-e2e-{datetime.now().strftime('%H:%M - %d/%m/%Y')}"
    state_path = tmp_path / "predefined-license-upload-state.json"
    submission = build_full_submission(name=name)

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
