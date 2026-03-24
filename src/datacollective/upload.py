from __future__ import annotations

import logging
from pathlib import Path


from datacollective.api_utils import (
    _enable_verbose,
)
from datacollective.upload_utils import (
    UploadState,
    MAX_UPLOAD_BYTES,
    _default_state_path,
    _load_or_create_state,
    _expected_parts,
    _normalize_parts,
    _init_progress_bar,
    _upload_missing_parts,
    _parts_from_mapping,
    _save_upload_state,
    _complete_upload,
    _cleanup_state_file,
)

logger = logging.getLogger(__name__)


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
    _save_upload_state(state_file, state)

    logger.info("Completing upload...")

    _complete_upload(state.fileUploadId, state.uploadId, state.parts, state.checksum)

    logger.info(f"Upload complete. File upload ID: {state.fileUploadId}")

    _cleanup_state_file(state_file)

    return state
