import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from datacollective.logging_utils import (
    _resolve_log_path,
    DEFAULT_LOG_FILENAME,
)


def test_resolve_log_path_no_env_var():
    with patch.dict(os.environ, {}, clear=False):
        if "MDC_LOG_PATH" in os.environ:
            del os.environ["MDC_LOG_PATH"]

        result = _resolve_log_path()
        expected = Path("~/.mozdata").expanduser() / DEFAULT_LOG_FILENAME
        assert result == expected


def test_resolve_log_path_env_var_as_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch.dict(os.environ, {"MDC_LOG_PATH": tmpdir}):
            result = _resolve_log_path()
            expected = Path(tmpdir) / DEFAULT_LOG_FILENAME
            assert result == expected


def test_resolve_log_path_env_var_as_file():
    with tempfile.NamedTemporaryFile() as tmpfile:
        with patch.dict(os.environ, {"MDC_LOG_PATH": tmpfile.name}):
            result = _resolve_log_path()
            expected = Path(tmpfile.name) / DEFAULT_LOG_FILENAME
            assert result == expected
