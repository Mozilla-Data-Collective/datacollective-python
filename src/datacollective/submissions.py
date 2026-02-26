from __future__ import annotations

import logging
from typing import Any


from datacollective.api_utils import _get_api_url, send_api_request, _enable_verbose
from datacollective.models import DatasetSubmission
from datacollective.upload import upload_dataset_file

logger = logging.getLogger(__name__)


def _ensure_submission_model(submission: DatasetSubmission) -> DatasetSubmission:
    if not isinstance(submission, DatasetSubmission):
        raise TypeError("`submission` must be a DatasetSubmission model")
    return submission


def _normalize_submission_id(submission_id: str) -> str:
    if not submission_id:
        raise ValueError("`submission_id` must be a non-empty string")
    return submission_id


DRAFT_FIELDS = {"name", "longDescription"}
UPDATE_FIELDS = {
    "shortDescription",
    "longDescription",
    "locale",
    "task",
    "format",
    "licenseAbbreviation",
    "license",
    "licenseUrl",
    "other",
    "restrictions",
    "forbiddenUsage",
    "additionalConditions",
    "pointOfContactFullName",
    "pointOfContactEmail",
    "fundedByFullName",
    "fundedByEmail",
    "legalContactFullName",
    "legalContactEmail",
    "createdByFullName",
    "createdByEmail",
    "intendedUsage",
    "ethicalReviewProcess",
    "exclusivityOptOut",
    "fileUploadId",
}
SUBMIT_FIELDS = {"agreeToSubmit"}


def _payload_for_fields(
    submission: DatasetSubmission, allowed_fields: set[str]
) -> dict[str, Any]:
    data = submission.model_dump(exclude_none=True)
    return {key: value for key, value in data.items() if key in allowed_fields}


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
    if "name" not in payload or "longDescription" not in payload:
        raise ValueError("`submission` must include `name` and `longDescription`")

    url = f"{_get_api_url()}/submissions"
    resp = send_api_request("POST", url, json_body=payload)
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
    submission_id = _normalize_submission_id(submission_id)

    payload = _payload_for_fields(submission, UPDATE_FIELDS)
    if not payload:
        raise ValueError("`submission` must include at least one updatable field")

    url = f"{_get_api_url()}/submissions/{submission_id}"
    resp = send_api_request("PATCH", url, json_body=payload)
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
    submission_id = _normalize_submission_id(submission_id)

    if submission.agreeToSubmit is not True:
        raise ValueError("`agreeToSubmit` must be True to submit a dataset")

    payload = _payload_for_fields(submission, SUBMIT_FIELDS)
    url = f"{_get_api_url()}/submissions/{submission_id}"
    resp = send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def create_submission_with_upload(
    file_path: str,
    submission: DatasetSubmission,
    submission_id: str | None = None,
    filename: str | None = None,
    state_path: str | None = None,
    resume: bool = True,
    verbose: bool = True,
    show_progress: bool = True,
) -> dict[str, Any]:
    """
    Convenience helper to create a submission, upload a file, update
    metadata, and submit for review.

    Args:
        file_path: Path to dataset archive.
        submission: Dataset submission model with metadata fields.
        submission_id: Optional existing submission ID to resume.
        filename: Optional filename override.
        state_path: Optional path to persist upload state.
        resume: Whether to resume a previous upload session.
        verbose: Whether to enable detailed logging during the process.
        show_progress: Whether to show a progress bar during upload.
    """
    _enable_verbose(verbose)

    submission = _ensure_submission_model(submission)

    if submission.agreeToSubmit is not True:
        raise ValueError("`agreeToSubmit` must be True to submit a dataset")

    if submission_id is None and submission.id:
        submission_id = submission.id

    if submission_id:
        submission_id = _normalize_submission_id(submission_id)
        logger.info(f"Using existing submission ID: {submission_id}")
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
        filename=filename,
        state_path=state_path,
        resume=resume,
        show_progress=show_progress,
    )

    if submission.fileUploadId and submission.fileUploadId != upload_state.fileUploadId:
        raise ValueError("`fileUploadId` does not match the completed upload")

    submission_with_upload = submission.model_copy(
        update={"fileUploadId": upload_state.fileUploadId}
    )

    logger.info("Updating submission metadata...")

    update_submission(submission_id, submission_with_upload)

    logger.info("Submitting dataset for review...")

    response = submit_submission(submission_id, submission)
    response.setdefault("fileUploadId", upload_state.fileUploadId)

    logger.info("Submission complete!")

    return response
