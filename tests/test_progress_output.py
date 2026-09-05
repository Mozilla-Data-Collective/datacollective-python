import io
import json
import logging
import sys
from functools import partial
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datacollective.download import DownloadPlan, _execute_download_plan
from datacollective.upload import upload_dataset_file, upload_sample_file
from datacollective.upload_utils import _init_progress_bar


class _Console(io.TextIOWrapper):
    def __init__(self, *, tty: bool, encoding: str = "utf-8") -> None:
        self._bytes = io.BytesIO()
        super().__init__(self._bytes, encoding=encoding, errors="strict", newline="")
        self._tty = tty

    def isatty(self) -> bool:
        return self._tty

    def getvalue(self) -> str:
        self.flush()
        return self._bytes.getvalue().decode(self.encoding)


@pytest.fixture
def console(monkeypatch):
    def configure(*, stdout_tty=False, stderr_tty=True, encoding="utf-8"):
        stdout = _Console(tty=stdout_tty, encoding=encoding)
        stderr = _Console(tty=stderr_tty, encoding=encoding)
        monkeypatch.setattr(sys, "stdout", stdout)
        monkeypatch.setattr(sys, "stderr", stderr)
        return stdout, stderr

    return configure


def _download(tmp_path: Path, monkeypatch, *, show_progress=True, resume=False):
    plan = DownloadPlan(
        download_url="https://storage.example.test/download",
        target_filepath=tmp_path / "dataset.tar.gz",
        tmp_filepath=tmp_path / "dataset.tar.gz.part",
        size_bytes=6,
        checksum="checksum",
        checksum_filepath=tmp_path / "dataset.tar.gz.checksum",
    )
    if resume:
        plan.tmp_filepath.write_bytes(b"abc")
    response = MagicMock()
    response.__enter__.return_value = response
    response.iter_content.return_value = [b"def"] if resume else [b"abc", b"def"]
    monkeypatch.setattr(
        "datacollective.download._send_api_request", MagicMock(return_value=response)
    )

    _execute_download_plan(
        plan,
        resume_download_checksum="checksum" if resume else None,
        show_progress=show_progress,
    )

    assert plan.tmp_filepath.read_bytes() == b"abcdef"
    print(json.dumps({"bytes": plan.size_bytes}))


def _upload(
    tmp_path: Path,
    monkeypatch,
    *,
    show_progress=True,
    upload_function=upload_dataset_file,
    enable_logging=False,
):
    path = tmp_path / "dataset.tar.gz"
    path.write_bytes(b"abcdef")
    response = MagicMock()
    response.json.return_value = {
        "fileUploadId": "file-upload",
        "uploadId": "upload",
        "url": "https://storage.example.test/part",
        "partNumber": 1,
    }
    response.headers = {"ETag": '"etag-1"'}
    monkeypatch.setattr(
        "datacollective.upload_utils._get_api_url", lambda: "https://api.example.test"
    )
    monkeypatch.setattr(
        "datacollective.upload_utils._send_api_request",
        MagicMock(return_value=response),
    )
    monkeypatch.setattr(
        "datacollective.upload_utils.requests.put", MagicMock(return_value=response)
    )

    state = upload_function(
        str(path),
        submission_id="submission",
        show_progress=show_progress,
        enable_logging=enable_logging,
    )

    assert len(state.parts) == 1
    assert state.parts[0].etag == "etag-1"
    print(json.dumps({"bytes": state.fileSize}))


@pytest.fixture(params=[_download, _upload], ids=["download", "upload"])
def transfer(request, tmp_path: Path, monkeypatch):
    return partial(request.param, tmp_path, monkeypatch)


@pytest.mark.parametrize(
    "stdout_tty", [False, True], ids=["piped-stdout", "tty-stdout"]
)
def test_progress_uses_interactive_stderr(transfer, console, stdout_tty):
    stdout, stderr = console(stdout_tty=stdout_tty)

    transfer()

    assert stdout.getvalue() == '{"bytes": 6}\n'
    progress = stderr.getvalue()
    assert "🦊" in progress
    assert "█" in progress
    assert "ETA:" in progress
    assert "100.0%" in progress
    assert "(6.0 B/6.0 B)" in progress
    assert progress.endswith("\n")


@pytest.mark.parametrize(
    "stdout_tty", [False, True], ids=["piped-stdout", "tty-stdout"]
)
def test_progress_is_silent_when_stderr_is_not_a_tty(transfer, console, stdout_tty):
    stdout, stderr = console(stdout_tty=stdout_tty, stderr_tty=False)

    transfer()

    assert stdout.getvalue() == '{"bytes": 6}\n'
    assert stderr.getvalue() == ""


def test_show_progress_false_disables_interactive_output(transfer, console):
    stdout, stderr = console()

    transfer(show_progress=False)

    assert stdout.getvalue() == '{"bytes": 6}\n'
    assert stderr.getvalue() == ""


@pytest.mark.parametrize("encoding", ["cp1252", "ascii"])
def test_progress_supports_legacy_console_encodings(transfer, console, encoding):
    stdout, stderr = console(encoding=encoding)

    transfer()

    assert stdout.getvalue() == '{"bytes": 6}\n'
    assert "100.0%" in stderr.getvalue()
    assert "(6.0 B/6.0 B)" in stderr.getvalue()
    assert stderr.getvalue().endswith("\n")


def test_resumed_download_renders_existing_and_completed_bytes(
    tmp_path: Path, monkeypatch, console
):
    stdout, stderr = console()

    _download(tmp_path, monkeypatch, resume=True)

    assert stdout.getvalue() == '{"bytes": 6}\n'
    assert "50.0% (3.0 B/6.0 B)" in stderr.getvalue()
    assert "100.0% (6.0 B/6.0 B)" in stderr.getvalue()


def test_resumed_upload_renders_existing_and_completed_bytes(console):
    stdout, stderr = console()

    progress_bar = _init_progress_bar(
        True, file_size=6, part_size=3, already_uploaded=1
    )
    assert progress_bar is not None
    progress_bar.update(3)
    progress_bar.finish()

    assert stdout.getvalue() == ""
    assert "50.0% (3.0 B/6.0 B)" in stderr.getvalue()
    assert "100.0% (6.0 B/6.0 B)" in stderr.getvalue()
    assert stderr.getvalue().endswith("\n")


def test_sample_upload_progress_uses_stderr(tmp_path: Path, monkeypatch, console):
    stdout, stderr = console()

    _upload(tmp_path, monkeypatch, upload_function=upload_sample_file)

    assert stdout.getvalue() == '{"bytes": 6}\n'
    assert "100.0% (6.0 B/6.0 B)" in stderr.getvalue()
    assert stderr.getvalue().endswith("\n")


def test_enabled_status_logging_uses_stderr_without_rendering_progress(
    tmp_path: Path, monkeypatch, console
):
    stdout, stderr = console(stderr_tty=False)
    package_logger = logging.getLogger("datacollective")
    previous_level = package_logger.level
    monkeypatch.setattr(package_logger, "handlers", [])
    monkeypatch.setattr(package_logger, "propagate", False)
    monkeypatch.setenv("MDC_LOG_PATH", str(tmp_path / "sdk.log"))
    try:
        _upload(tmp_path, monkeypatch, enable_logging=True)

        assert stdout.getvalue() == '{"bytes": 6}\n'
        assert "Uploading: dataset.tar.gz" in stderr.getvalue()
        assert "Upload complete." in stderr.getvalue()
        assert "\r" not in stderr.getvalue()
    finally:
        for handler in package_logger.handlers:
            handler.close()
        package_logger.setLevel(previous_level)
