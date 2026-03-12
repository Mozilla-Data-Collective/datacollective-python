from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from pathlib import Path
from typing import Any

import requests
from fox_progress_bar import ProgressBar
from pydantic import Field, ValidationError

from datacollective.api_utils import (
    _get_api_url,
    send_api_request,
    _format_bytes,
    _enable_verbose,
)
from datacollective.models import NonEmptyStrModel, UploadPart

logger = logging.getLogger(__name__)

# Longer read timeout for uploading potentially large chunks on slow connections
UPLOAD_TIMEOUT = (20, 600)  # (20s connect timeout, 10min read timeout)
# Retry configuration for part uploads
MAX_UPLOAD_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

DEFAULT_PART_SIZE = 5 * 1024 * 1024  # 5 MB default part size to upload chunk by chunk
DEFAULT_MIME_TYPE = "application/gzip"
MAX_UPLOAD_BYTES = 80 * 1000 * 1000 * 1000  # 80 GB


class UploadSession(NonEmptyStrModel):
    fileUploadId: str
    uploadId: str
    partSize: int = Field(..., gt=0)


class UploadState(NonEmptyStrModel):
    submissionId: str
    fileUploadId: str
    uploadId: str
    fileSize: int = Field(..., gt=0, le=MAX_UPLOAD_BYTES)
    partSize: int = Field(..., gt=0)
    filename: str
    mimeType: str
    parts: list[UploadPart] = Field(default_factory=list)
    checksum: str | None = None


class PresignedPartUrl(NonEmptyStrModel):
    partNumber: int = Field(..., ge=1)
    url: str
    expiresAt: str | None = None


class _UploadInitiatePayload(NonEmptyStrModel):
    submissionId: str
    filename: str
    fileSize: int = Field(..., gt=0, le=MAX_UPLOAD_BYTES)
    mimeType: str


class _PresignedPartRequest(NonEmptyStrModel):
    fileUploadId: str
    partNumber: int = Field(..., ge=1)


class _CompleteUploadPayload(NonEmptyStrModel):
    fileUploadId: str
    uploadId: str | None = None
    parts: list[UploadPart] = Field(..., min_length=1)
    checksum: str


def _initiate_upload(
    submission_id: str, filename: str, file_size: int, mime_type: str
) -> UploadSession:
    """
    Start a multipart upload for a dataset submission.

    Args:
        submission_id: Dataset submission ID.
        filename: Name of the file to upload.
        file_size: Size of the file in bytes.
        mime_type: MIME type for the file.
    """
    payload = _UploadInitiatePayload(
        submissionId=submission_id,
        filename=filename,
        fileSize=file_size,
        mimeType=mime_type,
    )
    url = f"{_get_api_url()}/uploads"
    resp = send_api_request("POST", url, json_body=payload.model_dump())
    data = dict(resp.json())
    session_payload = {
        "fileUploadId": str(data.get("fileUploadId", "")),
        "uploadId": str(data.get("uploadId", "")),
        "partSize": int(data.get("partSize", 0)) or DEFAULT_PART_SIZE,
    }
    try:
        return UploadSession.model_validate(session_payload)
    except ValidationError as exc:
        raise RuntimeError("Upload initiation did not return expected fields") from exc


def _get_presigned_part_url(file_upload_id: str, part_number: int) -> PresignedPartUrl:
    """
    Request a presigned URL for a specific multipart part.

    Args:
        file_upload_id: File upload ID.
        part_number: 1-based multipart part number.
    """
    request = _PresignedPartRequest(fileUploadId=file_upload_id, partNumber=part_number)
    url = f"{_get_api_url()}/uploads/{request.fileUploadId}/parts/{request.partNumber}"
    resp = send_api_request("GET", url)
    data = dict(resp.json())
    presigned_url = data.get("url") or data.get("presignedUrl")
    payload = {
        "partNumber": int(data.get("partNumber", request.partNumber)),
        "url": str(presigned_url or ""),
        "expiresAt": data.get("expiresAt") or None,
    }
    return PresignedPartUrl.model_validate(payload)


def _complete_upload(
    file_upload_id: str,
    upload_id: str | None,
    parts: list[UploadPart],
    checksum: str,
) -> dict[str, Any]:
    """
    Complete a multipart upload and persist the checksum.
    """
    request = _CompleteUploadPayload(
        fileUploadId=file_upload_id,
        uploadId=upload_id,
        parts=parts,
        checksum=checksum,
    )

    url = f"{_get_api_url()}/uploads/{request.fileUploadId}"
    payload = {
        "parts": [part.model_dump() for part in request.parts],
        "checksum": request.checksum,
    }
    if request.uploadId:
        payload["uploadId"] = request.uploadId
    resp = send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def upload_dataset_file(
    file_path: str,
    submission_id: str,
    state_path: str | None = None,
    verbose: bool = False,
    show_progress: bool = True,
) -> UploadState:
    """
    Upload a dataset file using multipart uploads with resumable state.

    Uploads are limited to 80GB and use the `application/gzip` MIME type.
    Pass the submission ID of the target dataset submission. This works for
    both draft submissions and for uploading a new `.tar.gz` version to an
    already approved dataset submission.

    Args:
        file_path: Path to the dataset archive on disk.
        submission_id: Dataset submission ID (not the dataset ID).
        state_path: Optional path to persist upload state. Defaults to
            `<filename>.mdc-upload.json` alongside the archive.
        verbose: Whether to enable detailed logging during the upload.
        show_progress: Whether to show a progress bar during upload.
    """
    _enable_verbose(verbose)

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: `{file_path}`")

    file_size = path.stat().st_size
    if file_size <= 0:
        raise ValueError("`file_path` must point to a non-empty file")
    if file_size > MAX_UPLOAD_BYTES:
        raise ValueError("`file_path` exceeds the 80GB upload limit")

    final_filename = path.name

    state_file = Path(state_path) if state_path else _default_state_path(path)

    state = _load_or_create_state(
        state_file=state_file,
        submission_id=submission_id,
        final_filename=final_filename,
        file_size=file_size,
    )

    expected_parts = _expected_parts(state.fileSize, state.partSize)

    parts_by_number = _normalize_parts(state)
    if parts_by_number:
        logger.info(
            f"Resuming: {len(parts_by_number)}/{expected_parts} parts already uploaded."
        )

    logger.info(f"Uploading file: {final_filename}")

    progress_bar = _init_progress_bar(
        show_progress=show_progress,
        file_size=state.fileSize,
        part_size=state.partSize,
        already_uploaded=len(parts_by_number),
    )

    bytes_read, checksum = _upload_missing_parts(
        path=path,
        state=state,
        parts_by_number=parts_by_number,
        expected_parts=expected_parts,
        progress_bar=progress_bar,
        state_file=state_file,
    )

    if progress_bar:
        progress_bar.finish()

    if bytes_read != state.fileSize:
        raise RuntimeError(
            "Upload aborted because file size changed during upload "
            f"(expected {state.fileSize} bytes, read {bytes_read})."
        )

    if len(parts_by_number) != expected_parts:
        raise RuntimeError(
            "Upload incomplete. Expected "
            f"{expected_parts} parts but have {len(parts_by_number)}."
        )

    state.checksum = checksum
    state.parts = _parts_from_mapping(parts_by_number)
    save_upload_state(state_file, state)

    logger.info("Completing upload...")

    _complete_upload(state.fileUploadId, state.uploadId, state.parts, state.checksum)

    logger.info(f"Upload complete. File upload ID: {state.fileUploadId}")

    _cleanup_state_file(state_file)

    return state


def load_upload_state(path: Path) -> UploadState | None:
    """Load persisted upload state from disk."""
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
        return UploadState.model_validate(payload)
    except Exception:
        return None


def save_upload_state(path: Path, state: UploadState) -> None:
    """Persist upload state to disk."""
    path.write_text(json.dumps(state.model_dump(), indent=2))


def _default_state_path(file_path: Path) -> Path:
    return file_path.with_name(file_path.name + ".mdc-upload.json")


def _load_or_create_state(
    state_file: Path,
    submission_id: str,
    final_filename: str,
    file_size: int,
) -> UploadState:
    state = load_upload_state(state_file)
    if state:
        if not _state_matches(state, submission_id, final_filename, file_size):
            logger.warning(
                "Upload state does not match file or submission. Restarting upload."
            )
            state = None
        else:
            logger.info(f"Resuming upload from `{str(state_file)}`")

    if not state:
        logger.info(
            f"Initiating upload for '{final_filename}' ({_format_bytes(file_size)})..."
        )
        session = _initiate_upload(
            submission_id, final_filename, file_size, DEFAULT_MIME_TYPE
        )
        state = UploadState(
            submissionId=submission_id,
            fileUploadId=session.fileUploadId,
            uploadId=session.uploadId,
            fileSize=file_size,
            partSize=session.partSize,
            filename=final_filename,
            mimeType=DEFAULT_MIME_TYPE,
            parts=[],
            checksum=None,
        )
        save_upload_state(state_file, state)

    return state


def _state_matches(
    state: UploadState, submission_id: str, filename: str, file_size: int
) -> bool:
    return (
        state.fileSize == file_size
        and state.filename == filename
        and state.submissionId == submission_id
        and state.mimeType == DEFAULT_MIME_TYPE
    )


def _init_progress_bar(
    show_progress: bool,
    file_size: int,
    part_size: int,
    already_uploaded: int,
) -> ProgressBar | None:
    if not show_progress:
        return None
    progress_bar = ProgressBar(file_size)
    if already_uploaded > 0:
        progress_bar.update(already_uploaded * part_size)
        progress_bar._display()
    return progress_bar


def _upload_missing_parts(
    path: Path,
    state: UploadState,
    parts_by_number: dict[int, str],
    expected_parts: int,
    progress_bar: ProgressBar | None,
    state_file: Path,
) -> tuple[int, str]:
    hasher = hashlib.sha256()
    bytes_read = 0
    with open(path, "rb") as file_handle:
        for part_index in range(expected_parts):
            part_number = part_index + 1
            chunk = bytearray(file_handle.read(state.partSize))
            if not chunk:
                break
            bytes_read += len(chunk)
            hasher.update(chunk)

            if part_number in parts_by_number:
                continue

            presigned = _get_presigned_part_url(state.fileUploadId, part_number)
            response = _upload_part_with_retry(presigned.url, chunk)
            etag = _extract_etag(response)
            parts_by_number[part_number] = etag
            state.parts = _parts_from_mapping(parts_by_number)
            save_upload_state(state_file, state)

            if progress_bar:
                progress_bar.update(len(chunk))

    return bytes_read, hasher.hexdigest()


def _expected_parts(file_size: int, part_size: int) -> int:
    return int(math.ceil(file_size / part_size))


def _normalize_parts(state: UploadState) -> dict[int, str]:
    return {part.partNumber: part.etag for part in state.parts}


def _parts_from_mapping(parts_by_number: dict[int, str]) -> list[UploadPart]:
    return [
        UploadPart(partNumber=number, etag=etag)
        for number, etag in sorted(parts_by_number.items())
    ]


def _cleanup_state_file(state_file: Path) -> None:
    try:
        if state_file.exists():
            state_file.unlink()
    except Exception:
        logger.debug(f"Failed to remove upload state file: {state_file}")


def _upload_part(presigned_url: str, payload: bytes) -> requests.Response:
    if not presigned_url:
        raise ValueError("Missing presigned URL for upload part")
    resp = requests.put(presigned_url, data=payload, timeout=UPLOAD_TIMEOUT)
    resp.raise_for_status()
    return resp


def _upload_part_with_retry(
    presigned_url: str,
    payload: bytes,
    max_retries: int = MAX_UPLOAD_RETRIES,
) -> requests.Response:
    """Upload a single part with automatic retries on transient failures."""
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return _upload_part(presigned_url, payload)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = RETRY_BACKOFF_SECONDS * attempt
                logger.debug(
                    f"Upload part attempt {attempt} failed, retrying in {wait}s..."
                )
                time.sleep(wait)
    raise RuntimeError(
        f"Failed to upload part after {max_retries} attempts"
    ) from last_exc


def _extract_etag(response: requests.Response) -> str:
    etag = response.headers.get("ETag")
    if not etag:
        raise RuntimeError("Missing ETag header in upload response")
    return etag.strip().strip('"')
