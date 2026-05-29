import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from datacollective.upload import upload_dataset_file
from datacollective.upload_utils import WARN_UPLOAD_BYTES


def test_upload_dataset_file_rejects_missing_file(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.tar.gz"

    with pytest.raises(FileNotFoundError, match="File not found"):
        upload_dataset_file(str(missing_file), submission_id="submission")


def test_upload_dataset_file_rejects_empty_file(tmp_path: Path) -> None:
    empty_file = tmp_path / "empty.tar.gz"
    empty_file.write_bytes(bytearray())

    with pytest.raises(ValueError, match="non-empty file"):
        upload_dataset_file(str(empty_file), submission_id="submission")


def test_upload_dataset_file_warns_for_oversized_file(tmp_path: Path) -> None:
    """Files above WARN_UPLOAD_BYTES should print a warning to stderr but not raise."""
    f = tmp_path / "big.tar.gz"
    f.write_bytes(b"x")

    stderr = io.StringIO()
    with patch("pathlib.Path.stat") as mock_stat:
        mock_stat.return_value = MagicMock(st_size=WARN_UPLOAD_BYTES + 1)
        with patch(
            "datacollective.upload._load_or_create_state",
            side_effect=StopIteration,
        ):
            with patch("sys.stderr", stderr):
                with pytest.raises(StopIteration):
                    upload_dataset_file(str(f), submission_id="submission")

    output = stderr.getvalue()
    assert "WARNING" in output
    assert "exceeds" in output
    assert "support@mozilladatacollective.com" in output


def test_upload_dataset_file_does_not_warn_at_exact_limit(tmp_path: Path) -> None:
    """Files exactly at WARN_UPLOAD_BYTES should not trigger the warning."""
    f = tmp_path / "exact.tar.gz"
    f.write_bytes(b"x")

    stderr = io.StringIO()
    with patch("pathlib.Path.stat") as mock_stat:
        mock_stat.return_value = MagicMock(st_size=WARN_UPLOAD_BYTES)
        with patch(
            "datacollective.upload._load_or_create_state",
            side_effect=StopIteration,
        ):
            with patch("sys.stderr", stderr):
                with pytest.raises(StopIteration):
                    upload_dataset_file(str(f), submission_id="submission")

    assert "WARNING" not in stderr.getvalue()
