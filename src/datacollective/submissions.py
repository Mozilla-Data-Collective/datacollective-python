from __future__ import annotations

from typing import Any

from datacollective.api_utils import _get_api_url, send_api_request
from datacollective.models import (
    DatasetSubmissionDraftInput,
    DatasetSubmissionSubmitInput,
)
from datacollective.upload import upload_dataset_file


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
    url = f"{_get_api_url()}/submissions"
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
    url = f"{_get_api_url()}/submissions/{submission_id}"
    payload = model.model_dump(exclude_none=True)
    resp = send_api_request("PATCH", url, json_body=payload)
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
    verbose: bool = True,
    show_progress: bool = True,
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
        verbose: Whether to print status messages during the process.
        show_progress: Whether to show a progress bar during upload.
    """
    if verbose:
        print(f"Creating submission draft for '{name}'...")

    draft = create_submission_draft(name=name, long_description=long_description)
    submission_id = draft.get("submissionId")
    if not submission_id:
        raise RuntimeError("Draft creation did not return a submissionId")

    if verbose:
        print(f"Draft created. Submission ID: {submission_id}")

    upload_state = upload_dataset_file(
        file_path=file_path,
        submission_id=submission_id,
        mime_type=mime_type,
        filename=filename,
        state_path=state_path,
        resume=resume,
        verbose=verbose,
        show_progress=show_progress,
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

    if verbose:
        print("Submitting dataset for review...")

    response = submit_submission(
        submission_id, DatasetSubmissionSubmitInput.model_validate(payload)
    )
    response.setdefault("fileUploadId", upload_state.fileUploadId)

    if verbose:
        print("Submission complete!")

    return response
