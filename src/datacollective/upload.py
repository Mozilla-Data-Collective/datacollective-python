from __future__ import annotations

from pathlib import Path

from datacollective.errors import ResourceRemovedError
from datacollective.logging_utils import (
    _enable_logging,
    get_logger,
)
from datacollective.upload_utils import (
    UploadState,
    _default_state_path,
    _load_or_create_state,
    _expected_parts,
    _normalize_parts,
    _init_progress_bar,
    _upload_parts_and_compute_checksum,
    _save_upload_state,
    _complete_upload,
    _cleanup_state_file,
    _ensure_part_size_is_valid,
    _ensure_max_workers_is_valid,
    _uploaded_bytes,
    DEFAULT_PART_SIZE,
    DEFAULT_MAX_WORKERS,
)

logger = get_logger(__name__)


def upload_dataset_file(
    file_path: str,
    submission_id: str,
    state_path: str | None = None,
    show_progress: bool = True,
    enable_logging: bool = False,
    part_size: int = DEFAULT_PART_SIZE,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> UploadState:
    """
    Upload a dataset file using multipart uploads with resumable state.

    Uploads use the `application/gzip` MIME type.
    Pass the submission ID of the target dataset submission. This works for
    both draft submissions and for uploading a new `.tar.gz` version to an
    already approved dataset submission.

    Args:
        file_path: Path to the dataset archive on disk.
        submission_id: Dataset submission ID (not the dataset ID).
        state_path: Optional path to persist upload state. Defaults to
            `<filename>.mdc-upload.json` alongside the archive.
        enable_logging: Whether to enable detailed logging during the upload.
        show_progress: Whether to show a progress bar during upload.
        part_size: Multipart part size in bytes. Ignored when resuming an
            existing upload, which keeps the part size recorded in its state file.
        max_workers: Number of parts uploaded concurrently. The file is still
            read and hashed in order; about `max_workers + 1` parts are held in
            memory. Use 1 to upload parts one at a time.
    """
    return _upload_file(
        file_path=file_path,
        submission_id=submission_id,
        state_path=state_path,
        show_progress=show_progress,
        enable_logging=enable_logging,
        part_size=part_size,
        max_workers=max_workers,
        is_sample=False,
    )


def upload_sample_file(
    file_path: str,
    submission_id: str,
    state_path: str | None = None,
    show_progress: bool = True,
    enable_logging: bool = False,
    part_size: int = DEFAULT_PART_SIZE,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> UploadState:
    """
    Upload an **optional** sample file for a dataset submission.

    A sample file is a small, representative excerpt of the dataset that
    users can inspect without downloading the full archive. It is uploaded
    exactly like the dataset archive (resumable multipart upload,
    `application/gzip` MIME type) but through the submission's sample endpoints,
    and it does not replace the dataset file.

    Args:
        file_path: Path to the sample archive on disk.
        submission_id: Dataset submission ID (not the dataset ID).
        state_path: Optional path to persist upload state. Defaults to
            `<filename>.mdc-sample-upload.json` alongside the archive.
        enable_logging: Whether to enable detailed logging during the upload.
        show_progress: Whether to show a progress bar during upload.
        part_size: Multipart part size in bytes. Ignored when resuming an
            existing upload, which keeps the part size recorded in its state file.
        max_workers: Number of parts uploaded concurrently. The file is still
            read and hashed in order; about `max_workers + 1` parts are held in
            memory. Use 1 to upload parts one at a time.
    """
    return _upload_file(
        file_path=file_path,
        submission_id=submission_id,
        state_path=state_path,
        show_progress=show_progress,
        enable_logging=enable_logging,
        part_size=part_size,
        max_workers=max_workers,
        is_sample=True,
    )


def _upload_file(
    file_path: str,
    submission_id: str,
    state_path: str | None,
    show_progress: bool,
    enable_logging: bool,
    part_size: int,
    max_workers: int,
    is_sample: bool,
) -> UploadState:
    """
    Shared multipart upload function for the dataset archive and the sample file.

    Args:
        file_path: Path to the archive on disk.
        submission_id: Dataset submission ID (not the dataset ID).
        state_path: Optional path to persist upload state.
        show_progress: Whether to show a progress bar during upload.
        enable_logging: Whether to enable detailed logging during the upload.
        part_size: Multipart part size in bytes.
        max_workers: Number of parts uploaded concurrently.
        is_sample: Whether to upload the file as the submission's sample file.
    """
    path = Path(file_path)
    _enable_logging(enable_logging)

    if not path.exists():
        raise FileNotFoundError(f"File not found: `{file_path}`")

    file_size = path.stat().st_size
    if file_size <= 0:
        raise ValueError("`file_path` must point to a non-empty file")

    _ensure_part_size_is_valid(file_size, part_size)
    _ensure_max_workers_is_valid(max_workers)

    state_file = (
        Path(state_path) if state_path else _default_state_path(path, is_sample)
    )

    try:
        return _run_upload(
            path=path,
            state_file=state_file,
            submission_id=submission_id,
            file_size=file_size,
            part_size=part_size,
            max_workers=max_workers,
            is_sample=is_sample,
            show_progress=show_progress,
        )
    except ResourceRemovedError:
        # The submission was deleted, so this upload can never be resumed.
        _cleanup_state_file(state_file)
        raise


def _run_upload(
    path: Path,
    state_file: Path,
    submission_id: str,
    file_size: int,
    part_size: int,
    max_workers: int,
    is_sample: bool,
    show_progress: bool,
) -> UploadState:
    """Upload the file, resuming from the state in `state_file` when it matches."""
    final_filename = path.name

    state = _load_or_create_state(
        state_file=state_file,
        submission_id=submission_id,
        final_filename=final_filename,
        file_size=file_size,
        part_size=part_size,
        is_sample=is_sample,
    )

    expected_parts = _expected_parts(state.fileSize, state.partSize)

    parts_by_number = _normalize_parts(state)
    if parts_by_number:
        logger.info(
            f"Resuming: {len(parts_by_number)}/{expected_parts} parts already uploaded."
        )

    logger.info(f"Uploading: {final_filename}")

    progress_bar = _init_progress_bar(
        show_progress=show_progress,
        file_size=state.fileSize,
        already_uploaded_bytes=_uploaded_bytes(
            parts_by_number, state.fileSize, state.partSize
        ),
    )

    bytes_read, checksum = _upload_parts_and_compute_checksum(
        path=path,
        state=state,
        parts_by_number=parts_by_number,
        expected_parts=expected_parts,
        progress_bar=progress_bar,
        state_file=state_file,
        max_workers=max_workers,
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
    _save_upload_state(state_file, state)

    logger.info("Completing upload...")

    _complete_upload(
        state.fileUploadId,
        state.uploadId,
        state.parts,
        state.checksum,
        state.submissionId,
        state.isSample,
    )

    logger.info(f"Upload complete. File upload ID: {state.fileUploadId}")

    _cleanup_state_file(state_file)

    return state
