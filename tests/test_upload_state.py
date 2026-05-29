import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from datacollective.upload import upload_dataset_file
from datacollective.upload_utils import MAX_UPLOAD_BYTES


def test_upload_dataset_file_rejects_missing_file(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.tar.gz"

    with pytest.raises(FileNotFoundError, match="File not found"):
        upload_dataset_file(str(missing_file), submission_id="submission")


def test_upload_dataset_file_rejects_empty_file(tmp_path: Path) -> None:
    empty_file = tmp_path / "empty.tar.gz"
    empty_file.write_bytes(bytearray())

    with pytest.raises(ValueError, match="non-empty file"):
        upload_dataset_file(str(empty_file), submission_id="submission")


def test_upload_dataset_file_warns_for_oversized_file(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Files above MAX_UPLOAD_BYTES should log a warning but not raise."""
    f = tmp_path / "big.tar.gz"
    f.write_bytes(b"x")

    with patch("pathlib.Path.stat") as mock_stat:
        mock_stat.return_value = MagicMock(st_size=MAX_UPLOAD_BYTES + 1)
        # Stop execution after the warning by making _load_or_create_state raise;
        # we only need to verify the warning fires, not that the upload completes.
        with patch(
            "datacollective.upload._load_or_create_state",
            side_effect=StopIteration,
        ):
            with caplog.at_level(logging.WARNING, logger="datacollective.upload"):
                with pytest.raises(StopIteration):
                    upload_dataset_file(str(f), submission_id="submission")

    warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("exceeds" in m for m in warning_messages)
    assert any("support@mozilladatacollective.com" in m for m in warning_messages)


def test_upload_dataset_file_does_not_warn_at_exact_limit(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Files exactly at MAX_UPLOAD_BYTES should not trigger the warning."""
    f = tmp_path / "exact.tar.gz"
    f.write_bytes(b"x")

    with patch("pathlib.Path.stat") as mock_stat:
        mock_stat.return_value = MagicMock(st_size=MAX_UPLOAD_BYTES)
        with patch(
            "datacollective.upload._load_or_create_state",
            side_effect=StopIteration,
        ):
            with caplog.at_level(logging.WARNING, logger="datacollective.upload"):
                with pytest.raises(StopIteration):
                    upload_dataset_file(str(f), submission_id="submission")

    warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert not any("exceeds" in m for m in warning_messages)
