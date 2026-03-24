from __future__ import annotations

import logging
from typing import Any


from datacollective.api_utils import _enable_logging, _get_api_url, _send_api_request
from datacollective.models import (
    DatasetSubmission,
    _ensure_submission_model,
    _payload_for_fields,
    DRAFT_FIELDS,
    UPDATE_FIELDS,
    _should_validate_local_final_submission,
    _validate_final_submission_fields,
    SUBMIT_FIELDS,
)
from datacollective.upload import upload_dataset_file
from datacollective.upload_utils import _resolve_upload_state

logger = logging.getLogger(__name__)


def create_submission_draft(submission: DatasetSubmission) -> dict[str, Any]:
    """
    Create a draft dataset submission.

    Args:
        submission: Dataset submission model containing at least `name`
            and `longDescription`.

    Returns:
        The full API response dict (contains a ``submission`` key with
        the created submission).
    """
    submission = _ensure_submission_model(submission)
    payload = _payload_for_fields(submission, DRAFT_FIELDS)
    if "name" not in payload:
        raise ValueError("`submission` must include `name`")

    url = f"{_get_api_url()}/submissions"
    resp = _send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def update_submission(
    submission_id: str,
    submission: DatasetSubmission,
) -> dict[str, Any]:
    """
    Update metadata on an existing dataset submission.

    Args:
        submission_id: Dataset submission ID.
        submission: Dataset submission model containing update fields.

    Returns:
        The full API response dict (contains a ``submission`` key).
    """
    submission = _ensure_submission_model(submission)

    payload = _payload_for_fields(submission, UPDATE_FIELDS)
    if not payload:
        raise ValueError("`submission` must include at least one updatable field")

    url = f"{_get_api_url()}/submissions/{submission_id}"
    resp = _send_api_request("PATCH", url, json_body=payload)
    return dict(resp.json())


def submit_submission(
    submission_id: str,
    submission: DatasetSubmission,
) -> dict[str, Any]:
    """
    Submit a dataset submission for review.

    Args:
        submission_id: Dataset submission ID.
        submission: Dataset submission model with `agreeToSubmit=True`.

    Returns:
        The full API response dict (contains a ``submission`` key with
        the submission whose status should be ``"submitted"``).
    """
    submission = _ensure_submission_model(submission)

    if _should_validate_local_final_submission(submission):
        _validate_final_submission_fields(submission, require_file_upload_id=True)
    elif submission.agreeToSubmit is not True:
        raise ValueError("`agreeToSubmit` must be True to submit a dataset")

    payload = _payload_for_fields(submission, SUBMIT_FIELDS)
    url = f"{_get_api_url()}/submissions/{submission_id}"
    resp = _send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def create_submission_with_upload(
    file_path: str,
    submission: DatasetSubmission,
    state_path: str | None = None,
    enable_logging: bool = False,
) -> dict[str, Any]:
    """
    Single point function to create a submission, upload a file, update metadata, and submit for review.
    Allows for resuming an upload if interrupted by persisting state to a file.

    Args:
        file_path: Path to dataset archive.
        submission: Dataset submission model with metadata fields.
        state_path: Optional path to persist upload state.
        enable_logging: Whether to enable detailed logging during the process.
    """
    _enable_logging(enable_logging)

    submission = _ensure_submission_model(submission)

    _validate_final_submission_fields(submission, require_file_upload_id=False)

    state_file, existing_upload_state = _resolve_upload_state(file_path, state_path)

    if existing_upload_state:
        submission_id = existing_upload_state.submissionId
        logger.info(
            f"Found existing upload state at `{state_file}`. Resuming submission {submission_id}."
        )
    else:
        logger.info(f"Creating submission draft for '{submission.name}'...")

        draft = create_submission_draft(submission)

        submission_payload = draft.get("submission", {})
        submission_id = (
            submission_payload.get("id")
            if isinstance(submission_payload, dict)
            else None
        )
        if not submission_id:
            raise RuntimeError("Draft creation did not return a submission id")

        logger.info(f"Draft created. Submission ID: {submission_id}")

    upload_state = upload_dataset_file(
        file_path=file_path,
        submission_id=submission_id,
        state_path=state_path,
        enable_logging=enable_logging,
    )

    submission.fileUploadId = upload_state.fileUploadId
    _validate_final_submission_fields(submission, require_file_upload_id=True)

    logger.info("Updating submission metadata...")

    update_submission(submission_id, submission)

    logger.info("Submitting dataset for review...")

    response = submit_submission(submission_id, submission)

    logger.info("Submission complete!")

    return response
