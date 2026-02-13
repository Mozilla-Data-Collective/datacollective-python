from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from datacollective.api_utils import HTTP_TIMEOUT, _get_api_url, send_api_request
from datacollective.models import UploadPart


@dataclass(frozen=True)
class UploadSession:
    fileUploadId: str
    uploadId: str
    bucket: str
    key: str
    partSize: int
    partNumber: int
    presignedUrl: str
    expiresAt: str


@dataclass
class UploadState:
    submissionId: str
    fileUploadId: str
    uploadId: str
    fileSize: int
    partSize: int
    filename: str
    mimeType: str
    parts: list[UploadPart]
    checksum: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "submissionId": self.submissionId,
            "fileUploadId": self.fileUploadId,
            "uploadId": self.uploadId,
            "fileSize": self.fileSize,
            "partSize": self.partSize,
            "filename": self.filename,
            "mimeType": self.mimeType,
            "parts": [part.model_dump() for part in self.parts],
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "UploadState":
        return cls(
            submissionId=str(payload.get("submissionId", "")),
            fileUploadId=str(payload.get("fileUploadId", "")),
            uploadId=str(payload.get("uploadId", "")),
            fileSize=int(payload.get("fileSize", 0)),
            partSize=int(payload.get("partSize", 0)),
            filename=str(payload.get("filename", "")),
            mimeType=str(payload.get("mimeType", "")),
            parts=[
                UploadPart.model_validate(part) for part in payload.get("parts", [])
            ],
            checksum=payload.get("checksum"),
        )


@dataclass(frozen=True)
class PresignedPartUrl:
    partNumber: int
    presignedUrl: str
    expiresAt: str


def initiate_upload(
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
    _require_non_empty(submission_id, "submission_id")
    _require_non_empty(filename, "filename")
    _require_non_empty(mime_type, "mime_type")
    if file_size <= 0:
        raise ValueError("`file_size` must be a positive integer")

    url = f"{_get_api_url()}/upload/initiate"
    payload = {
        "submissionId": submission_id,
        "filename": filename,
        "fileSize": file_size,
        "mimeType": mime_type,
    }
    resp = send_api_request("POST", url, json_body=payload)
    data = dict(resp.json())
    return UploadSession(
        fileUploadId=str(data.get("fileUploadId", "")),
        uploadId=str(data.get("uploadId", "")),
        bucket=str(data.get("bucket", "")),
        key=str(data.get("key", "")),
        partSize=int(data.get("partSize", 0)),
        partNumber=int(data.get("partNumber", 1)),
        presignedUrl=str(data.get("presignedUrl", "")),
        expiresAt=str(data.get("expiresAt", "")),
    )


def get_presigned_part_url(file_upload_id: str, chunk_index: int) -> PresignedPartUrl:
    """
    Request a presigned URL for a specific multipart chunk.

    Args:
        file_upload_id: File upload ID.
        chunk_index: Zero-based chunk index.
    """
    _require_non_empty(file_upload_id, "file_upload_id")
    if chunk_index < 0:
        raise ValueError("`chunk_index` must be a non-negative integer")

    url = f"{_get_api_url()}/upload/presigned-url"
    resp = send_api_request(
        "GET",
        url,
        params={"fileUploadId": file_upload_id, "chunkIndex": chunk_index},
    )
    data = dict(resp.json())
    return PresignedPartUrl(
        partNumber=int(data.get("partNumber", chunk_index + 1)),
        presignedUrl=str(data.get("presignedUrl", "")),
        expiresAt=str(data.get("expiresAt", "")),
    )


def complete_upload(
    file_upload_id: str, upload_id: str, parts: list[UploadPart], checksum: str
) -> dict[str, Any]:
    """
    Complete a multipart upload and persist the checksum.
    """
    _require_non_empty(file_upload_id, "file_upload_id")
    _require_non_empty(upload_id, "upload_id")
    _require_non_empty(checksum, "checksum")
    if not parts:
        raise ValueError("`parts` must contain at least one uploaded part")

    url = f"{_get_api_url()}/upload/complete"
    payload = {
        "fileUploadId": file_upload_id,
        "uploadId": upload_id,
        "parts": [part.model_dump() for part in parts],
        "checksum": checksum,
    }
    resp = send_api_request("POST", url, json_body=payload)
    return dict(resp.json())


def upload_dataset_file(
    file_path: str,
    submission_id: str,
    mime_type: str,
    filename: str | None = None,
    state_path: str | None = None,
    resume: bool = True,
) -> UploadState:
    """
    Upload a dataset file using multipart uploads with resumable state.

    Args:
        file_path: Path to the dataset archive on disk.
        submission_id: Dataset submission ID.
        mime_type: MIME type for the file.
        filename: Optional filename override for the upload.
        state_path: Optional path to persist upload state. Defaults to
            `<filename>.mdc-upload.json` alongside the archive.
        resume: Whether to reuse an existing upload state file.
    """
    _require_non_empty(file_path, "file_path")
    _require_non_empty(submission_id, "submission_id")
    _require_non_empty(mime_type, "mime_type")

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: `{file_path}`")

    file_size = path.stat().st_size
    if file_size <= 0:
        raise ValueError("`file_path` must point to a non-empty file")

    final_filename = filename or path.name
    state_file = Path(state_path) if state_path else _default_state_path(path)

    state = load_upload_state(state_file) if resume else None
    if state:
        if (
            state.fileSize != file_size
            or state.filename != final_filename
            or state.submissionId != submission_id
        ):
            print("Upload state does not match file or submission. Restarting upload.")
            state = None
        else:
            print(f"Resuming upload from `{str(state_file)}`")

    session: UploadSession | None = None
    if not state:
        session = initiate_upload(submission_id, final_filename, file_size, mime_type)
        if not session.fileUploadId or not session.uploadId or session.partSize <= 0:
            raise RuntimeError("Upload initiation did not return expected fields")
        state = UploadState(
            submissionId=submission_id,
            fileUploadId=session.fileUploadId,
            uploadId=session.uploadId,
            fileSize=file_size,
            partSize=session.partSize,
            filename=final_filename,
            mimeType=mime_type,
            parts=[],
            checksum=None,
        )
        save_upload_state(state_file, state)

    expected_parts = _expected_parts(state.fileSize, state.partSize)
    if expected_parts <= 0:
        raise RuntimeError("Invalid upload configuration (expected parts <= 0)")

    parts_by_number = _normalize_parts(state)

    hasher = hashlib.sha256()
    bytes_read = 0
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

            presigned_url = ""
            if session and part_number == session.partNumber and session.presignedUrl:
                presigned_url = session.presignedUrl
            else:
                presigned = get_presigned_part_url(
                    state.fileUploadId, chunk_index=part_index
                )
                presigned_url = presigned.presignedUrl

            response = _upload_part(presigned_url, chunk)
            etag = _extract_etag(response)
            parts_by_number[part_number] = etag
            state.parts.append(UploadPart(partNumber=part_number, etag=etag))
            save_upload_state(state_file, state)

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

    state.checksum = hasher.hexdigest()
    state.parts = [
        UploadPart(partNumber=number, etag=etag)
        for number, etag in sorted(parts_by_number.items())
    ]
    save_upload_state(state_file, state)

    complete_upload(state.fileUploadId, state.uploadId, state.parts, state.checksum)
    return state


def load_upload_state(path: Path) -> UploadState | None:
    """Load persisted upload state from disk."""
    if not path.exists():
        return None
    payload = json.loads(path.read_text())
    try:
        state = UploadState.from_dict(payload)
    except Exception:
        return None
    if not state.fileUploadId or not state.uploadId:
        return None
    if state.fileSize <= 0 or state.partSize <= 0:
        return None
    return state


def save_upload_state(path: Path, state: UploadState) -> None:
    """Persist upload state to disk."""
    path.write_text(json.dumps(state.to_dict(), indent=2))


def _default_state_path(file_path: Path) -> Path:
    return file_path.with_name(file_path.name + ".mdc-upload.json")


def _expected_parts(file_size: int, part_size: int) -> int:
    if file_size <= 0 or part_size <= 0:
        return 0
    return int(math.ceil(file_size / part_size))


def _normalize_parts(state: UploadState) -> dict[int, str]:
    parts_by_number: dict[int, str] = {}
    for part in state.parts:
        if part.partNumber <= 0:
            continue
        parts_by_number[part.partNumber] = part.etag
    return parts_by_number


def _upload_part(presigned_url: str, payload: bytes) -> requests.Response:
    if not presigned_url:
        raise ValueError("Missing presigned URL for upload part")
    resp = requests.put(presigned_url, data=payload, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp


def _extract_etag(response: requests.Response) -> str:
    etag = response.headers.get("ETag")
    if not etag:
        raise RuntimeError("Missing ETag header in upload response")
    return etag.strip().strip('"')


def _require_non_empty(value: str, field_name: str) -> None:
    if not value or not str(value).strip():
        raise ValueError(f"`{field_name}` must be a non-empty string")
