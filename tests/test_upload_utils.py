import logging
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datacollective.models import UploadPart
from datacollective.upload_utils import (
    ENV_MAX_CONCURRENT_PARTS,
    ENV_PART_SIZE,
    MAX_PARTS,
    UploadState,
    _ProgressChunk,
    _load_upload_state,
    _minimum_part_size,
    _resolve_max_concurrent_parts,
    _resolve_part_size,
    _save_upload_state,
)


# ---------------------------------------------------------------------------
# UploadState persistence
# ---------------------------------------------------------------------------

def test_upload_state_round_trip(tmp_path: Path) -> None:
    state_path = tmp_path / "upload-state.json"
    state = UploadState(
        submissionId="submission",
        fileUploadId="file-upload",
        uploadId="upload-id",
        fileSize=1024,
        partSize=256,
        filename="dataset.tar.gz",
        mimeType="application/gzip",
        parts=[UploadPart(partNumber=1, etag="etag-1")],
        checksum="abc123",
    )

    _save_upload_state(state_path, state)
    loaded = _load_upload_state(state_path)

    assert loaded is not None
    assert loaded.fileUploadId == state.fileUploadId
    assert loaded.parts[0].partNumber == 1
    assert loaded.parts[0].etag == "etag-1"


def test_load_upload_state_returns_none_for_invalid_payload(tmp_path: Path) -> None:
    state_path = tmp_path / "upload-state.json"
    state_path.write_text(
        """{
  \"submissionId\": \"submission\",
  \"fileUploadId\": \"file-upload\",
  \"uploadId\": \"\",
  \"fileSize\": 1024,
  \"partSize\": 256,
  \"filename\": \"dataset.tar.gz\",
  \"mimeType\": \"application/gzip\",
  \"parts\": []
}"""
    )

    assert _load_upload_state(state_path) is None


# ---------------------------------------------------------------------------
# _resolve_part_size
# ---------------------------------------------------------------------------

def test_resolve_part_size_returns_explicit_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_PART_SIZE, "9999")
    assert _resolve_part_size(1234) == 1234


def test_resolve_part_size_reads_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_PART_SIZE, "9999")
    assert _resolve_part_size(None) == 9999


def test_resolve_part_size_returns_none_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_PART_SIZE, raising=False)
    assert _resolve_part_size(None) is None


def test_resolve_part_size_warns_and_returns_none_for_invalid_env_var(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv(ENV_PART_SIZE, "not-a-number")
    with caplog.at_level(logging.WARNING):
        result = _resolve_part_size(None)
    assert result is None
    assert ENV_PART_SIZE in caplog.text


# ---------------------------------------------------------------------------
# _resolve_max_concurrent_parts
# ---------------------------------------------------------------------------

def test_resolve_max_concurrent_parts_returns_explicit_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV_MAX_CONCURRENT_PARTS, "8")
    assert _resolve_max_concurrent_parts(4) == 4


def test_resolve_max_concurrent_parts_reads_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV_MAX_CONCURRENT_PARTS, "4")
    assert _resolve_max_concurrent_parts(None) == 4


def test_resolve_max_concurrent_parts_defaults_to_1_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV_MAX_CONCURRENT_PARTS, raising=False)
    assert _resolve_max_concurrent_parts(None) == 1


def test_resolve_max_concurrent_parts_warns_and_defaults_for_invalid_env_var(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv(ENV_MAX_CONCURRENT_PARTS, "not-a-number")
    with caplog.at_level(logging.WARNING):
        result = _resolve_max_concurrent_parts(None)
    assert result == 1
    assert ENV_MAX_CONCURRENT_PARTS in caplog.text


# ---------------------------------------------------------------------------
# _ProgressChunk
# ---------------------------------------------------------------------------

def test_progress_chunk_reports_bytes_as_read() -> None:
    bar = MagicMock()
    chunk = _ProgressChunk(b"hello world", bar)

    chunk.read(5)
    chunk.read(6)

    assert chunk.bytes_reported == 11
    assert bar.update.call_count == 2
    bar.update.assert_any_call(5)
    bar.update.assert_any_call(6)


def test_progress_chunk_full_read_reports_all_bytes() -> None:
    bar = MagicMock()
    data = b"x" * 1000
    chunk = _ProgressChunk(data, bar)

    chunk.read()  # read all at once

    assert chunk.bytes_reported == 1000
    bar.update.assert_called_once_with(1000)


def test_progress_chunk_empty_read_does_not_update_bar() -> None:
    bar = MagicMock()
    chunk = _ProgressChunk(b"hello", bar)

    chunk.read(5)   # exhaust
    chunk.read(5)   # now empty

    assert chunk.bytes_reported == 5
    assert bar.update.call_count == 1


def test_progress_chunk_len_returns_data_length() -> None:
    chunk = _ProgressChunk(b"hello", MagicMock())
    assert len(chunk) == 5


# ---------------------------------------------------------------------------
# _minimum_part_size
# ---------------------------------------------------------------------------

def test_minimum_part_size_returns_exact_ceiling() -> None:
    """Part count must not exceed MAX_PARTS."""
    file_size = 150 * 1_000 * 1_000 * 1_000  # 150 GB
    result = _minimum_part_size(file_size)
    assert result == 15_000_000  # math.ceil(150 GB / 10_000)
    assert file_size / result <= MAX_PARTS


def test_minimum_part_size_small_file_is_one_byte() -> None:
    """For tiny files the minimum is trivially 1 byte (1 part)."""
    assert _minimum_part_size(1) == 1


def test_minimum_part_size_exact_multiple() -> None:
    """When file_size is an exact multiple of MAX_PARTS, no rounding occurs."""
    file_size = MAX_PARTS * 1_000_000  # each part exactly 1 MB
    assert _minimum_part_size(file_size) == 1_000_000


def test_minimum_part_size_rounds_up() -> None:
    """A file one byte larger than an exact multiple must round up."""
    file_size = MAX_PARTS * 1_000_000 + 1
    result = _minimum_part_size(file_size)
    assert result == 1_000_001
    assert file_size / result <= MAX_PARTS


# ---------------------------------------------------------------------------

def test_progress_chunk_uses_lock_when_provided() -> None:
    bar = MagicMock()
    lock = threading.Lock()
    chunk = _ProgressChunk(b"hello", bar, lock=lock)

    chunk.read(5)

    assert chunk.bytes_reported == 5
    bar.update.assert_called_once_with(5)
