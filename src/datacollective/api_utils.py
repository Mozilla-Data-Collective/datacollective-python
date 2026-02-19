from __future__ import annotations

import logging
import os
import platform
from pathlib import Path
from typing import Any

import requests
from dotenv import find_dotenv, load_dotenv

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "https://datacollective.mozillafoundation.org/api"
ENV_API_KEY = "MDC_API_KEY"
ENV_API_URL = "MDC_API_URL"
ENV_DOWNLOAD_PATH = "MDC_DOWNLOAD_PATH"
HTTP_TIMEOUT = (10, 60)  # (connect, read)

RATE_LIMIT_ERROR = "Rate limit exceeded. Please try again later."

load_dotenv(find_dotenv())


def send_api_request(
    method: str,
    url: str,
    stream: bool = False,
    extra_headers: dict[str, str] | None = None,
    timeout: tuple[int, int] | None = HTTP_TIMEOUT,
    include_auth_headers: bool = True,
    json_body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> requests.Response:
    """
    Send an HTTP request to the MDC API with appropriate headers and error handling.

    Args:
        method: HTTP method (e.g., 'GET', 'POST').
        url: Full URL for the API endpoint.
        stream: Whether to stream the response (default: False).
        extra_headers: Additional headers to include in the request (default: None). E.g. for resuming
        timeout: A tuple specifying (connect timeout, read timeout) in seconds (default: None).
        include_auth_headers: Whether to include authentication (API KEY) headers (default: True).
        json_body: Optional JSON body to send with the request.
        params: Optional query parameters to include in the request.

    Returns:
        The HTTP response object.

    Raises:
        FileNotFoundError: If the resource is not found (404).
        PermissionError: If access is denied (403).
        RuntimeError: If rate limit is exceeded (429).
        ValueError: If API key is missing when authentication is required.
        requests.HTTPError: For other non-2xx responses.
    """
    headers = {"User-Agent": _get_user_agent()}
    if include_auth_headers:
        headers.update(_auth_headers())
    if extra_headers:
        headers.update(extra_headers)

    logger.debug(f"API request: {method.upper()} {url} (stream={stream})")

    resp = requests.request(
        method=method.upper(),
        url=url,
        stream=stream,
        headers=headers,
        timeout=timeout,
        json=json_body,
        params=params,
    )

    if resp.status_code == 404:
        raise FileNotFoundError("Dataset not found")
    if resp.status_code == 403:
        raise PermissionError(
            "Access denied. Private dataset requires organization membership"
        )
    if resp.status_code == 429:
        raise RuntimeError(RATE_LIMIT_ERROR)
    resp.raise_for_status()

    return resp


def _get_api_url() -> str:
    return os.getenv(ENV_API_URL, DEFAULT_API_URL).rstrip("/")


def _get_api_key() -> str:
    key = os.getenv(ENV_API_KEY)
    if not key:
        raise ValueError(
            f"Missing API key. Set env {ENV_API_KEY} to your MDC API token."
        )
    return key


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_get_api_key()}"}


def _get_user_agent() -> str:
    """Generate a user agent string with SDK name, version, and Python runtime info."""
    # Import here to avoid circular dependency
    try:
        from datacollective import __version__
    except ImportError:
        __version__ = "unknown"

    python_version = platform.python_version()
    system = platform.system()
    return f"datacollective-python/{__version__} (Python {python_version}; {system})"


def _prepare_download_headers(
    tmp_path: Path, resume_checksum: str | None
) -> tuple[dict[str, str], int]:
    """
    Prepare headers for download plan and determine existing file size for download resumption.

    Args:
        tmp_path: Path to the temporary file for download.
        resume_checksum: Checksum string to verify for resuming download (if any).

    Returns:
        A tuple containing:
        - A dict of headers to include in the download request.
        - The size of the existing file in bytes (0 if not resuming).
    """
    if not tmp_path.exists():  # invalid path
        return {}, 0

    if resume_checksum:
        existing_size = tmp_path.stat().st_size
        return {"Range": f"bytes={existing_size}-"}, existing_size

    tmp_path.unlink()  # remove existing file if no resume checksum supplied
    return {}, 0
