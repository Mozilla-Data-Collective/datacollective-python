from __future__ import annotations

import logging
from pathlib import Path
from typing import Any


from datacollective.api_utils import _get_api_url, send_api_request, _enable_verbose
from datacollective.models import DatasetSubmission, License
from datacollective.upload import _default_state_path, load_upload_state, upload_dataset_file

logger = logging.getLogger(__name__)


def _ensure_submission_model(submission: DatasetSubmission) -> DatasetSubmission:
    if not isinstance(submission, DatasetSubmission):
        raise TypeError("`submission` must be a DatasetSubmission model")
    return submission


DRAFT_FIELDS = {"name"}
UPDATE_FIELDS = {
    "name",
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
    "agreeToSubmit"
}
SUBMIT_FIELDS = {"agreeToSubmit"}


def _payload_for_fields(
    submission: DatasetSubmission, allowed_fields: set[str]
) -> dict[str, Any]:
    data = submission.model_dump(mode="json", exclude_none=True)
    payload = {key: value for key, value in data.items() if key in allowed_fields}

    if "licenseAbbreviation" in allowed_fields and isinstance(submission.license, License):
        payload["licenseAbbreviation"] = submission.license.value
        # Remove custom license fields if a predefined license is used
        payload.pop("license", None)
        payload.pop("licenseUrl", None)

    return payload


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

    if submission.agreeToSubmit is not True:
        raise ValueError("`agreeToSubmit` must be True to submit a dataset")

    payload = _payload_for_fields(submission, SUBMIT_FIELDS)
    url = f"{_get_api_url()}/submissions/{submission_id}"
    resp = send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def _resolve_upload_state(file_path: str, state_path: str | None) -> tuple[Path, Any | None]:
    state_file = Path(state_path) if state_path else _default_state_path(Path(file_path))
    return state_file, load_upload_state(state_file)


def create_submission_with_upload(
    file_path: str,
    submission: DatasetSubmission,
    state_path: str | None = None,
    verbose: bool = True,
) -> dict[str, Any]:
    """
    Single point function to create a submission, upload a file, update metadata, and submit for review.
    Allows for resuming an upload if interrupted by persisting state to a file.

    Args:
        file_path: Path to dataset archive.
        submission: Dataset submission model with metadata fields.
        state_path: Optional path to persist upload state.
        verbose: Whether to enable detailed logging during the process.
    """
    _enable_verbose(verbose)

    submission = _ensure_submission_model(submission)

    if submission.agreeToSubmit is not True:
        raise ValueError("`agreeToSubmit` must be True to submit a dataset")

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
            submission_payload.get("id") if isinstance(submission_payload, dict) else None
        )
        if not submission_id:
            raise RuntimeError("Draft creation did not return a submission id")

        logger.info(f"Draft created. Submission ID: {submission_id}")

    upload_state = upload_dataset_file(
        file_path=file_path,
        submission_id=submission_id,
        state_path=state_path,
    )

    submission.fileUploadId = upload_state.fileUploadId

    logger.info("Updating submission metadata...")

    update_submission(submission_id, submission)

    logger.info("Submitting dataset for review...")

    response = submit_submission(submission_id, submission)

    logger.info("Submission complete!")

    return response
