from __future__ import annotations

import logging
from typing import Any

from datacollective.api_utils import _get_api_url, send_api_request, _enable_verbose
from datacollective.models import (
    DatasetSubmissionDraftInput,
    DatasetSubmissionSubmitInput,
    DatasetSubmissionUpdateInput,
)
from datacollective.upload import upload_dataset_file

logger = logging.getLogger(__name__)


def create_submission_draft(name: str, long_description: str) -> dict[str, Any]:
    """
    Create a draft dataset submission.

    Args:
        name: Dataset name.
        long_description: Full dataset description.

    Returns:
        The full API response dict (contains a ``submission`` key with
        the created submission).
    """
    payload = DatasetSubmissionDraftInput(
        name=name, longDescription=long_description
    ).model_dump()
    url = f"{_get_api_url()}/submissions"
    resp = send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def update_submission(
    submission_id: str,
    update_fields: DatasetSubmissionUpdateInput | dict[str, Any],
) -> dict[str, Any]:
    """
    Update metadata on an existing dataset submission.

    Args:
        submission_id: Dataset submission ID.
        update_fields: Fields to update (all optional).

    Returns:
        The full API response dict (contains a ``submission`` key).
    """
    if not submission_id or not submission_id.strip():
        raise ValueError("`submission_id` must be a non-empty string")

    model = DatasetSubmissionUpdateInput.model_validate(update_fields)
    url = f"{_get_api_url()}/submissions/{submission_id}"
    payload = model.model_dump(exclude_none=True)
    resp = send_api_request("PATCH", url, json_body=payload)
    return dict(resp.json())


def submit_submission(
    submission_id: str,
    agree_to_submit: bool = True,
) -> dict[str, Any]:
    """
    Submit a dataset submission for review.

    Args:
        submission_id: Dataset submission ID.
        agree_to_submit: Acknowledge agreement to submit (must be True).

    Returns:
        The full API response dict (contains a ``submission`` key with
        the submission whose status should be ``"submitted"``).
    """
    if not submission_id or not submission_id.strip():
        raise ValueError("`submission_id` must be a non-empty string")

    model = DatasetSubmissionSubmitInput(agreeToSubmit=agree_to_submit)
    url = f"{_get_api_url()}/submissions/{submission_id}"
    payload = model.model_dump()
    resp = send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def create_submission_with_upload(
    file_path: str,
    name: str,
    long_description: str,
    submission_fields: DatasetSubmissionUpdateInput | dict[str, Any],
    mime_type: str,
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
        name: Dataset name.
        long_description: Full dataset description.
        submission_fields: Metadata fields for the submission update step.
        mime_type: MIME type for the file.
        filename: Optional filename override.
        state_path: Optional path to persist upload state.
        resume: Whether to resume a previous upload session.
        verbose: Whether to enable detailed logging during the process.
        show_progress: Whether to show a progress bar during upload.
    """
    _enable_verbose(verbose)

    logger.info(f"Creating submission draft for '{name}'...")

    draft = create_submission_draft(name=name, long_description=long_description)
    submission = draft.get("submission", {})
    submission_id = submission.get("id") if isinstance(submission, dict) else None
    if not submission_id:
        raise RuntimeError("Draft creation did not return a submission id")

    logger.info(f"Draft created. Submission ID: {submission_id}")

    upload_state = upload_dataset_file(
        file_path=file_path,
        submission_id=submission_id,
        mime_type=mime_type,
        filename=filename,
        state_path=state_path,
        resume=resume,
        show_progress=show_progress,
    )

    if isinstance(submission_fields, DatasetSubmissionUpdateInput):
        update_payload = submission_fields.model_dump(exclude_none=True)
    else:
        update_payload = dict(submission_fields)

    if (
        update_payload.get("fileUploadId")
        and update_payload["fileUploadId"] != upload_state.fileUploadId
    ):
        raise ValueError("`fileUploadId` does not match the completed upload")
    update_payload["fileUploadId"] = upload_state.fileUploadId

    logger.info("Updating submission metadata...")

    update_submission(
        submission_id,
        DatasetSubmissionUpdateInput.model_validate(update_payload),
    )

    logger.info("Submitting dataset for review...")

    response = submit_submission(submission_id, agree_to_submit=True)
    response.setdefault("fileUploadId", upload_state.fileUploadId)

    logger.info("Submission complete!")

    return response
