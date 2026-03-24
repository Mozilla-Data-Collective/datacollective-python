import logging
import os
import uuid
import warnings
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path


logger = logging.getLogger(__name__)

PACKAGE_LOGGER_NAME = "datacollective"
_PKG_LOGGER = logging.getLogger(PACKAGE_LOGGER_NAME)
DEFAULT_LOG_FILENAME = "datacollective.log"
DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5
_SESSION_ID = (
    f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    f"-pid{os.getpid()}-{uuid.uuid4().hex[:8]}"
)
_CONSOLE_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [session=%(session_id)s] %(name)s: %(message)s"
)
_FILE_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [session=%(session_id)s] "
    "%(name)s:%(lineno)d: %(message)s"
)


class _SessionContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.session_id = _SESSION_ID
        return True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def configure_package_logging() -> None:
    if not any(
        isinstance(handler, logging.NullHandler) for handler in _PKG_LOGGER.handlers
    ):
        _PKG_LOGGER.addHandler(logging.NullHandler())


def _enable_logging(enable_logging: bool) -> None:
    if not enable_logging:
        return None

    log_path = Path("~/.mozdata").expanduser() / DEFAULT_LOG_FILENAME
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if not any(
        type(handler) is logging.StreamHandler for handler in _PKG_LOGGER.handlers
    ):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(_CONSOLE_LOG_FORMAT))
        console_handler.addFilter(_SessionContextFilter())
        _PKG_LOGGER.addHandler(console_handler)

    if not any(
        isinstance(handler, logging.FileHandler)
        and Path(handler.baseFilename) == log_path
        for handler in _PKG_LOGGER.handlers
    ):
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=DEFAULT_LOG_MAX_BYTES,
            backupCount=DEFAULT_LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(_FILE_LOG_FORMAT))
        file_handler.addFilter(_SessionContextFilter())
        _PKG_LOGGER.addHandler(file_handler)

    _PKG_LOGGER.setLevel(logging.DEBUG)
    logger.debug(
        "Detailed local logging enabled at `%s` (session=%s, max_bytes=%s, backups=%s)",
        log_path,
        _SESSION_ID,
        DEFAULT_LOG_MAX_BYTES,
        DEFAULT_LOG_BACKUP_COUNT,
    )
    return True


configure_package_logging()
