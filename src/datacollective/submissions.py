from __future__ import annotations

from typing import Any

from datacollective.api_utils import _get_api_url, send_api_request
from datacollective.models import (
    DatasetSubmissionDraftInput,
    DatasetSubmissionSubmitInput,
)
from datacollective.uploads import upload_dataset_file


def create_submission_draft(name: str, long_description: str) -> dict[str, Any]:
    """
    Create a draft dataset submission.

    Args:
        name: Dataset name.
        long_description: Full dataset description.
    """
    payload = DatasetSubmissionDraftInput(
        name=name, longDescription=long_description
    ).model_dump()
    url = f"{_get_api_url()}/datasets/submission"
    resp = send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def submit_submission(
    submission_id: str, submission_fields: DatasetSubmissionSubmitInput | dict[str, Any]
) -> dict[str, Any]:
    """
    Submit a dataset submission for review.

    Args:
        submission_id: Dataset submission ID.
        submission_fields: Datasheet fields required for submission.
    """
    if not submission_id or not submission_id.strip():
        raise ValueError("`submission_id` must be a non-empty string")

    model = DatasetSubmissionSubmitInput.model_validate(submission_fields)
    url = f"{_get_api_url()}/datasets/submission/{submission_id}/submit"
    resp = send_api_request("POST", url, json_body=model.model_dump())
    return dict(resp.json())


def create_submission_with_upload(
    file_path: str,
    name: str,
    long_description: str,
    submission_fields: DatasetSubmissionSubmitInput | dict[str, Any],
    mime_type: str,
    filename: str | None = None,
    state_path: str | None = None,
    resume: bool = True,
) -> dict[str, Any]:
    """
    Convenience helper to create a submission, upload a file, and submit.

    Args:
        file_path: Path to dataset archive.
        name: Dataset name.
        long_description: Full dataset description.
        submission_fields: Datasheet fields required for submission.
        mime_type: MIME type for the file.
        filename: Optional filename override.
        state_path: Optional path to persist upload state.
        resume: Whether to resume a previous upload session.
    """
    draft = create_submission_draft(name=name, long_description=long_description)
    submission_id = draft.get("submissionId")
    if not submission_id:
        raise RuntimeError("Draft creation did not return a submissionId")

    upload_state = upload_dataset_file(
        file_path=file_path,
        submission_id=submission_id,
        mime_type=mime_type,
        filename=filename,
        state_path=state_path,
        resume=resume,
    )

    if isinstance(submission_fields, DatasetSubmissionSubmitInput):
        payload = submission_fields.model_dump()
    else:
        payload = dict(submission_fields)

    if (
        payload.get("fileUploadId")
        and payload["fileUploadId"] != upload_state.fileUploadId
    ):
        raise ValueError("`fileUploadId` does not match the completed upload")
    payload["fileUploadId"] = upload_state.fileUploadId

    response = submit_submission(
        submission_id, DatasetSubmissionSubmitInput.model_validate(payload)
    )
    response.setdefault("fileUploadId", upload_state.fileUploadId)
    return response
