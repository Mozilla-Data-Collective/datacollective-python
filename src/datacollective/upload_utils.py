import hashlib
import json
import math
import os
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path

import requests
from fox_progress_bar import ProgressBar
from pydantic import Field, ValidationError
from requests.adapters import HTTPAdapter

from datacollective.errors import RateLimitError
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

DEFAULT_PART_SIZE = 10 * 1024 * 1024  # 10 MB default part size to upload chunk by chunk
# Number of parts uploaded concurrently. Memory use is roughly
# (DEFAULT_MAX_WORKERS + 1) * part_size, and every part costs one presigned-URL
# request to the API, so keep the default modest to stay clear of rate limits.
DEFAULT_MAX_WORKERS = 4
DEFAULT_MIME_TYPE = "application/gzip"

# Suffixes of the resumable state files, kept separate so that uploading a
# dataset archive and a sample file never share the same state file.
STATE_FILE_SUFFIX = ".mdc-upload.json"
SAMPLE_STATE_FILE_SUFFIX = ".mdc-sample-upload.json"

# Storage requires every part except the last to be at least 5 MB
MINIMUM_PART_SIZE = 5 * 1024 * 1024
# Storage caps a multipart upload at 10.000 presigned parts
MAX_UPLOAD_PARTS = 10_000

RATE_LIMIT_HINT = (
    "A high `max_workers` value and/or a small `part_size` increase the chance "
    "of getting rate limited. Consider adjusting these values in your upload "
    "script and try again; the upload will resume from the parts already uploaded."
)


def _ensure_part_size_is_valid(file_size: int, part_size: int) -> None:
    """
    Ensure the part size is valid for the upload.

    The part size must be at least ``MINIMUM_PART_SIZE`` (the storage backend
    rejects parts smaller than this, except the last one) and large enough that
    the file does not require more than ``MAX_UPLOAD_PARTS`` parts. For example,
    if MAX_UPLOAD_PARTS is 10.000 and file_size is 100 GB, the part size must be
    minimum 10 MB, otherwise the upload will fail when requesting presigned URLs
    for parts above the limit.

    Raises a clear error up front instead of letting the storage backend reject
    an undersized part or a part number above the presigned-URL limit mid-upload.
    """
    if part_size < MINIMUM_PART_SIZE:
        raise ValueError(
            f"`part_size` must be at least {_format_bytes(MINIMUM_PART_SIZE)}, "
            f"got {_format_bytes(part_size)}."
        )
    required_parts = _expected_parts(file_size, part_size)
    if required_parts > MAX_UPLOAD_PARTS:
        min_part_size = int(math.ceil(file_size / MAX_UPLOAD_PARTS))
        raise ValueError(
            f"File requires {required_parts} parts at a part size of "
            f"{_format_bytes(part_size)} for the whole file of {_format_bytes(file_size)},"
            f" exceeding the limit of {MAX_UPLOAD_PARTS}. Increase the "
            f"`part_size` argument to at least {_format_bytes(min_part_size)}."
        )


def _ensure_max_workers_is_valid(max_workers: int) -> None:
    """Ensure at least one part can be uploaded at a time."""
    if max_workers < 1:
        raise ValueError(f"`max_workers` must be at least 1, got {max_workers}.")


class UploadSession(NonEmptyStrModel):
    fileUploadId: str
    uploadId: str


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
    isSample: bool = False


class PresignedPartUrl(NonEmptyStrModel):
    partNumber: int = Field(..., ge=1)
    url: str


class _UploadInitiatePayload(NonEmptyStrModel):
    submissionId: str
    filename: str
    fileSize: int = Field(..., gt=0)
    mimeType: str


class _CompleteUploadPayload(NonEmptyStrModel):
    fileUploadId: str
    uploadId: str | None = None
    parts: list[UploadPart] = Field(..., min_length=1)
    checksum: str


def _upload_base_url(submission_id: str, is_sample: bool) -> str:
    """
    Base URL of the multipart upload endpoints.

    The dataset archive and the optional sample file are uploaded the same way,
    only through different endpoints.

    Args:
        submission_id: Dataset submission ID.
        is_sample: Whether the upload targets the sample file endpoints instead
            of the dataset archive ones.
    """
    if is_sample:
        return f"{_get_api_url()}/submissions/{submission_id}/sample"
    return f"{_get_api_url()}/uploads"


def _initiate_upload(
    submission_id: str,
    filename: str,
    file_size: int,
    is_sample: bool = False,
) -> UploadSession:
    """
    Start a multipart upload for a dataset submission.

    Args:
        submission_id: Dataset submission ID.
        filename: Name of the file to upload.
        file_size: Size of the file in bytes.
        is_sample: Whether to upload the file as the submission's sample file.
    """
    payload = _UploadInitiatePayload(
        submissionId=submission_id,
        filename=filename,
        fileSize=file_size,
        mimeType=DEFAULT_MIME_TYPE,
    )
    url = _upload_base_url(submission_id, is_sample)
    resp = _send_api_request("POST", url, json_body=payload.model_dump())
    data = resp.json()
    try:
        return UploadSession(
            fileUploadId=data.get("fileUploadId", ""),
            uploadId=data.get("uploadId", ""),
        )
    except ValidationError as exc:
        raise RuntimeError("Upload initiation did not return expected fields") from exc


def _get_presigned_part_url(
    file_upload_id: str,
    part_number: int,
    submission_id: str,
    is_sample: bool = False,
) -> PresignedPartUrl:
    """
    Request a presigned URL for a specific multipart part.

    Args:
        file_upload_id: File upload ID.
        part_number: 1-based multipart part number.
        submission_id: Dataset submission ID.
        is_sample: Whether the part belongs to a sample file upload.
    """
    base_url = _upload_base_url(submission_id, is_sample)
    url = f"{base_url}/{file_upload_id}/parts/{part_number}"
    resp = _send_api_request("GET", url)
    return PresignedPartUrl(partNumber=part_number, url=resp.json().get("url", ""))


def _complete_upload(
    file_upload_id: str,
    upload_id: str | None,
    parts: list[UploadPart],
    checksum: str,
    submission_id: str,
    is_sample: bool = False,
) -> None:
    """
    Complete a multipart upload and persist the checksum.
    """
    request = _CompleteUploadPayload(
        fileUploadId=file_upload_id,
        uploadId=upload_id,
        parts=parts,
        checksum=checksum,
    )
    base_url = _upload_base_url(submission_id, is_sample)
    url = f"{base_url}/{request.fileUploadId}"
    # `fileUploadId` goes in the URL, not the body
    payload = request.model_dump(exclude={"fileUploadId"}, exclude_none=True)
    try:
        _send_api_request("POST", url, json_body=payload)
    except RateLimitError as exc:
        raise RateLimitError(response=exc.response) from exc


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
    """
    Persist upload state to disk.

    The state is written to a temporary file and then renamed over the real
    one, so an interruption (e.g. Ctrl-C) mid-write never leaves a truncated
    state file behind that would force the upload to restart from scratch.
    """
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(state.model_dump(), indent=2))
    os.replace(tmp_path, path)


def _default_state_path(file_path: Path, is_sample: bool = False) -> Path:
    suffix = SAMPLE_STATE_FILE_SUFFIX if is_sample else STATE_FILE_SUFFIX
    return file_path.with_name(file_path.name + suffix)


def _load_or_create_state(
    state_file: Path,
    submission_id: str,
    final_filename: str,
    file_size: int,
    part_size: int,
    is_sample: bool = False,
) -> UploadState:
    state = _load_upload_state(state_file)
    if state:
        if not _state_matches(
            state, submission_id, final_filename, file_size, is_sample
        ):
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
        session = _initiate_upload(submission_id, final_filename, file_size, is_sample)
        state = UploadState(
            submissionId=submission_id,
            fileUploadId=session.fileUploadId,
            uploadId=session.uploadId,
            fileSize=file_size,
            partSize=part_size,
            filename=final_filename,
            mimeType=DEFAULT_MIME_TYPE,
            isSample=is_sample,
        )
        _save_upload_state(state_file, state)

    return state


def _state_matches(
    state: UploadState,
    submission_id: str,
    filename: str,
    file_size: int,
    is_sample: bool = False,
) -> bool:
    return (
        state.fileSize == file_size
        and state.filename == filename
        and state.submissionId == submission_id
        and state.mimeType == DEFAULT_MIME_TYPE
        and state.isSample == is_sample
    )


def _uploaded_bytes(
    parts_by_number: dict[int, str], file_size: int, part_size: int
) -> int:
    """
    Number of bytes covered by the already uploaded parts.

    Every part is ``part_size`` bytes except the last one, which only holds the
    remainder of the file, so the parts cannot simply be multiplied by the
    part size.
    """
    return sum(
        min(part_size, file_size - (number - 1) * part_size)
        for number in parts_by_number
    )


def _init_progress_bar(
    show_progress: bool,
    file_size: int,
    already_uploaded_bytes: int,
) -> ProgressBar | None:
    if not show_progress:
        return None
    progress_bar = ProgressBar(file_size)
    if already_uploaded_bytes > 0:
        progress_bar.update(already_uploaded_bytes)
        progress_bar._display()
    return progress_bar


def _storage_session(max_workers: int) -> requests.Session:
    """
    HTTP session for the part uploads to storage.

    Reusing connections saves a TCP and TLS handshake per part. The pool must
    hold at least one connection per worker, otherwise urllib3 discards the
    extra connections after every request.
    """
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=max_workers)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _upload_single_part(
    file_upload_id: str,
    submission_id: str,
    is_sample: bool,
    part_number: int,
    chunk: bytes,
    session: requests.Session | None = None,
) -> str:
    """
    Upload one part and return its ETag. Runs on a worker thread.

    The presigned URL is requested right before the PUT because the URLs are
    short-lived.
    """
    try:
        presigned = _get_presigned_part_url(
            file_upload_id, part_number, submission_id, is_sample
        )
        response = _upload_part_with_retry(presigned.url, chunk, session=session)
    except RateLimitError as exc:
        raise RateLimitError(response=exc.response, hint=RATE_LIMIT_HINT) from exc
    return _extract_etag(response)


def _record_finished_parts(
    pending: dict[Future[str], tuple[int, int]],
    parts_by_number: dict[int, str],
    state: UploadState,
    state_file: Path,
) -> None:
    """
    After a failure, keep the parts that still finished so the upload can be
    resumed. Never raises: the original error must propagate unchanged.
    """
    for future, (part_number, _) in pending.items():
        if future.done() and not future.cancelled() and future.exception() is None:
            parts_by_number[part_number] = future.result()
    try:
        state.parts = _parts_from_mapping(parts_by_number)
        _save_upload_state(state_file, state)
    except Exception:
        logger.debug("Could not persist upload state after failure", exc_info=True)


def _upload_parts_and_compute_checksum(
    path: Path,
    state: UploadState,
    parts_by_number: dict[int, str],
    expected_parts: int,
    progress_bar: ProgressBar | None,
    state_file: Path,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> tuple[int, str]:
    """
    Read the whole file part by part, uploading the parts not yet in
    *parts_by_number* and hashing every part (uploaded now or earlier).

    The file is read and hashed in order on the calling thread, while up to
    *max_workers* parts are uploaded concurrently by a thread pool. Only the
    calling thread touches *parts_by_number*, the state file and the progress
    bar, so no locking is needed. At most *max_workers* parts (plus the one
    being read) are held in memory at any time.

    *parts_by_number* and the state file are updated after each uploaded part
    so an interrupted upload can resume. On the first failure no further parts
    are submitted, the parts that already finished are persisted, and the
    original exception is re-raised.

    Returns:
        The number of bytes read and the SHA-256 hex digest of the file.
    """
    hasher = hashlib.sha256()
    bytes_read = 0
    # Submitted but not yet recorded parts: future -> (part number, chunk size)
    pending: dict[Future[str], tuple[int, int]] = {}

    def record(done: set[Future[str]]) -> None:
        for future in done:
            part_number, size = pending.pop(future)
            # Re-raises the worker's exception with its original type
            parts_by_number[part_number] = future.result()
            state.parts = _parts_from_mapping(parts_by_number)
            _save_upload_state(state_file, state)
            if progress_bar:
                progress_bar.update(size)

    # Not used as a context manager on purpose: `__exit__` always waits for the
    # running parts, which would make Ctrl-C hang until they finish.
    executor = ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="mdc-upload"
    )
    # Closed in `finally` rather than by a `with` block so that it stays open
    # while the error path below waits for the in-flight parts.
    session = _storage_session(max_workers)
    try:
        with open(path, "rb") as file_handle:
            for part_index in range(expected_parts):
                part_number = part_index + 1
                chunk = file_handle.read(state.partSize)
                if not chunk:
                    break
                bytes_read += len(chunk)
                hasher.update(chunk)

                if part_number in parts_by_number:
                    continue

                if len(pending) >= max_workers:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    record(done)

                future = executor.submit(
                    _upload_single_part,
                    state.fileUploadId,
                    state.submissionId,
                    state.isSample,
                    part_number,
                    chunk,
                    session,
                )
                pending[future] = (part_number, len(chunk))
                # The executor now holds the only reference to the chunk
                del chunk

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                record(done)
        executor.shutdown(wait=True)
    except BaseException as exc:
        # Fail fast: drop the queued parts and keep the finished ones for
        # resuming. On ordinary errors wait for the in-flight parts so their
        # ETags are kept too; on Ctrl-C return right away instead.
        executor.shutdown(wait=isinstance(exc, Exception), cancel_futures=True)
        _record_finished_parts(pending, parts_by_number, state, state_file)
        raise
    finally:
        session.close()

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


def _upload_part(
    presigned_url: str,
    payload: bytes,
    session: requests.Session | None = None,
) -> requests.Response:
    put = session.put if session else requests.put
    resp = put(presigned_url, data=payload, timeout=UPLOAD_TIMEOUT)
    if resp.status_code == 429:
        raise RateLimitError(response=resp)
    resp.raise_for_status()
    return resp


def _resolve_upload_state(
    file_path: str, state_path: str | None, is_sample: bool = False
) -> tuple[Path, UploadState | None]:
    state_file = (
        Path(state_path)
        if state_path
        else _default_state_path(Path(file_path), is_sample)
    )
    return state_file, _load_upload_state(state_file)


def _upload_part_with_retry(
    presigned_url: str,
    payload: bytes,
    max_retries: int = MAX_UPLOAD_RETRIES,
    session: requests.Session | None = None,
) -> requests.Response:
    """Upload a single part with automatic retries on transient failures."""
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return _upload_part(presigned_url, payload, session=session)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            if attempt < max_retries:
                wait_seconds = RETRY_BACKOFF_SECONDS * attempt
                logger.debug(
                    f"Upload part attempt {attempt} failed, retrying in {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
    raise RuntimeError(
        f"Failed to upload part after {max_retries} attempts"
    ) from last_exc


def _extract_etag(response: requests.Response) -> str:
    etag = response.headers.get("ETag")
    if not etag:
        raise RuntimeError("Missing ETag header in upload response")
    return etag.strip().strip('"')
