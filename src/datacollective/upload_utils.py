from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from pathlib import Path
from typing import Any

import requests
from fox_progress_bar import ProgressBar
from pydantic import Field, ValidationError

from datacollective.api_utils import (
    _get_api_url,
    _send_api_request,
    _format_bytes,
)
from datacollective.logging_utils import get_logger
from datacollective.models import NonEmptyStrModel, UploadPart


logger = get_logger(__name__)

# Longer read timeout for uploading potentially large chunks on slow connections
UPLOAD_TIMEOUT = (20, 600)  # (20s connect timeout, 10min read timeout)
# Retry configuration for part uploads
MAX_UPLOAD_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

DEFAULT_PART_SIZE = 5 * 1024 * 1024  # 5 MB default part size to upload chunk by chunk
DEFAULT_MIME_TYPE = "application/gzip"
MAX_UPLOAD_BYTES = 150 * 1000 * 1000 * 1000  # 150 GB

ENV_PART_SIZE = "MDC_PART_SIZE"
ENV_MAX_CONCURRENT_PARTS = "MDC_MAX_CONCURRENT_PARTS"


def _resolve_part_size(explicit: int | None) -> int | None:
    """Return the effective part size: explicit arg → MDC_PART_SIZE env var → None (use server default)."""
    if explicit is not None:
        return explicit
    raw = os.getenv(ENV_PART_SIZE)
    if raw:
        try:
            return int(raw)
        except ValueError:
            logger.warning(f"Ignoring invalid {ENV_PART_SIZE} value '{raw}' (expected an integer).")
    return None


def _resolve_max_concurrent_parts(explicit: int | None) -> int:
    """Return the effective concurrency: explicit arg → MDC_MAX_CONCURRENT_PARTS env var → 1 (sequential)."""
    if explicit is not None:
        return explicit
    raw = os.getenv(ENV_MAX_CONCURRENT_PARTS)
    if raw:
        try:
            return int(raw)
        except ValueError:
            logger.warning(
                f"Ignoring invalid {ENV_MAX_CONCURRENT_PARTS} value '{raw}' (expected an integer)."
            )
    return 1


class _ProgressChunk:
    """File-like wrapper around a bytes chunk that reports upload progress as data is read.

    urllib3 calls ``read()`` in ~16 KB increments as it writes to the socket, so
    progress updates are smooth and continuous rather than jumping by a whole part
    at a time.  A ``lock`` should be supplied when multiple threads share the same
    ``ProgressBar`` instance.
    """

    def __init__(
        self,
        data: bytes,
        progress_bar: "ProgressBar",
        lock: threading.Lock | None = None,
    ) -> None:
        self._data = data
        self._pos = 0
        self._progress_bar = progress_bar
        self._lock = lock
        self.bytes_reported: int = 0

    def __len__(self) -> int:
        return len(self._data)

    def read(self, size: int = -1) -> bytes:
        remaining = len(self._data) - self._pos
        if size < 0 or size > remaining:
            size = remaining
        if size == 0:
            return b""
        chunk = self._data[self._pos : self._pos + size]
        self._pos += size
        with (self._lock or contextlib.nullcontext()):
            self._progress_bar.update(size)
        self.bytes_reported += size
        return chunk


class UploadSession(NonEmptyStrModel):
    fileUploadId: str
    uploadId: str
    partSize: int = Field(..., gt=0)


class UploadState(NonEmptyStrModel):
    submissionId: str
    fileUploadId: str
    uploadId: str
    fileSize: int = Field(..., gt=0)
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
    fileSize: int = Field(..., gt=0)
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
    resp = _send_api_request("POST", url, json_body=payload.model_dump())
    data = dict(resp.json())
    print(data)
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
    resp = _send_api_request("GET", url)
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
    resp = _send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def _load_upload_state(path: Path) -> UploadState | None:
    """Load persisted upload state from disk."""
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
        return UploadState.model_validate(payload)
    except Exception:
        return None


def _save_upload_state(path: Path, state: UploadState) -> None:
    """Persist upload state to disk."""
    path.write_text(json.dumps(state.model_dump(), indent=2))


def _default_state_path(file_path: Path) -> Path:
    return file_path.with_name(file_path.name + ".mdc-upload.json")


def _load_or_create_state(
    state_file: Path,
    submission_id: str,
    final_filename: str,
    file_size: int,
    part_size: int | None = None,
) -> UploadState:
    state = _load_upload_state(state_file)
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
            partSize=part_size if part_size is not None else session.partSize,
            filename=final_filename,
            mimeType=DEFAULT_MIME_TYPE,
            parts=[],
            checksum=None,
        )
        _save_upload_state(state_file, state)

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
    max_workers: int = 1,
) -> tuple[int, str]:
    hasher = hashlib.sha256()
    bytes_read = 0
    # Shared lock ensures only one thread updates the progress bar or state file at a time
    state_lock = threading.Lock()
    progress_lock = threading.Lock() if (max_workers > 1 and progress_bar) else None

    def _upload_single_part(part_number: int, chunk: bytes) -> tuple[int, str]:
        presigned = _get_presigned_part_url(state.fileUploadId, part_number)
        response = _upload_part_with_retry(
            presigned.url, chunk, progress_bar=progress_bar, progress_lock=progress_lock
        )
        return part_number, _extract_etag(response)

    def _record_completed_part(future: Any) -> None:
        part_num, etag = future.result()
        with state_lock:
            parts_by_number[part_num] = etag
            state.parts = _parts_from_mapping(parts_by_number)
            _save_upload_state(state_file, state)

    with open(path, "rb") as file_handle:
        if max_workers <= 1:
            for part_index in range(expected_parts):
                part_number = part_index + 1
                chunk = file_handle.read(state.partSize)
                if not chunk:
                    break
                bytes_read += len(chunk)
                hasher.update(chunk)
                if part_number in parts_by_number:
                    continue
                presigned = _get_presigned_part_url(state.fileUploadId, part_number)
                response = _upload_part_with_retry(
                    presigned.url, chunk, progress_bar=progress_bar
                )
                etag = _extract_etag(response)
                parts_by_number[part_number] = etag
                state.parts = _parts_from_mapping(parts_by_number)
                _save_upload_state(state_file, state)
        else:
            pending: dict[Any, int] = {}  # future -> chunk_size
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for part_index in range(expected_parts):
                    part_number = part_index + 1
                    chunk = file_handle.read(state.partSize)
                    if not chunk:
                        break
                    bytes_read += len(chunk)
                    hasher.update(chunk)
                    if part_number in parts_by_number:
                        continue
                    # Drain one completed future before reading the next chunk
                    # to keep at most max_workers chunks in memory at once
                    while len(pending) >= max_workers:
                        done, _ = wait(pending, return_when=FIRST_COMPLETED)
                        for f in done:
                            _record_completed_part(f)
                            del pending[f]
                    future = executor.submit(_upload_single_part, part_number, chunk)
                    pending[future] = len(chunk)
                # Drain remaining in-flight parts
                for f in as_completed(list(pending)):
                    _record_completed_part(f)

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


def _upload_part(presigned_url: str, payload: Any) -> requests.Response:
    if not presigned_url:
        raise ValueError("Missing presigned URL for upload part")
    resp = requests.put(presigned_url, data=payload, timeout=UPLOAD_TIMEOUT)
    resp.raise_for_status()
    return resp


def _resolve_upload_state(
    file_path: str, state_path: str | None
) -> tuple[Path, Any | None]:
    state_file = (
        Path(state_path) if state_path else _default_state_path(Path(file_path))
    )
    return state_file, _load_upload_state(state_file)


def _upload_part_with_retry(
    presigned_url: str,
    payload: bytes,
    max_retries: int = MAX_UPLOAD_RETRIES,
    progress_bar: "ProgressBar | None" = None,
    progress_lock: threading.Lock | None = None,
) -> requests.Response:
    """Upload a single part with automatic retries on transient failures.

    When a ``progress_bar`` is supplied, bytes are reported as they flow
    through the socket (~16 KB at a time) rather than only on completion.
    On retry, any progress reported by the failed attempt is rolled back so
    the bar stays accurate.
    """
    last_exc: Exception | None = None
    prev_reported: int = 0

    for attempt in range(1, max_retries + 1):
        # Roll back progress reported by the previous failed attempt
        if prev_reported > 0 and progress_bar is not None:
            with (progress_lock or contextlib.nullcontext()):
                progress_bar.downloaded = max(0, progress_bar.downloaded - prev_reported)
            prev_reported = 0

        data: Any = (
            _ProgressChunk(payload, progress_bar, progress_lock)
            if progress_bar is not None
            else payload
        )
        try:
            return _upload_part(presigned_url, data)
        except (requests.ConnectionError, requests.Timeout) as exc:
            prev_reported = data.bytes_reported if isinstance(data, _ProgressChunk) else 0
            last_exc = exc
            if attempt < max_retries:
                wait_time = RETRY_BACKOFF_SECONDS * attempt
                logger.debug(
                    f"Upload part attempt {attempt} failed, retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code < 500:
                raise
            prev_reported = data.bytes_reported if isinstance(data, _ProgressChunk) else 0
            last_exc = exc
            if attempt < max_retries:
                wait_time = RETRY_BACKOFF_SECONDS * attempt
                logger.debug(
                    f"Upload part attempt {attempt} failed with server error "
                    f"({exc.response.status_code if exc.response is not None else 'unknown'}), "
                    f"retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
    raise RuntimeError(
        f"Failed to upload part after {max_retries} attempts"
    ) from last_exc


def _extract_etag(response: requests.Response) -> str:
    etag = response.headers.get("ETag")
    if not etag:
        raise RuntimeError("Missing ETag header in upload response")
    return etag.strip().strip('"')
