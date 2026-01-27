from __future__ import annotations

import os
import platform
from typing import Any

import requests
from dotenv import find_dotenv, load_dotenv

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
    *,
    headers: dict[str, str] | None = None,
    timeout: tuple[int, int] | None = None,
    raise_known_errors: bool = True,
    **kwargs: Any,
) -> requests.Response:
    """
    Send an HTTP request with default MDC auth headers and timeout, and
    normalize common error codes (403/404/429) to exceptions.
    """
    default_headers = {
        "User-Agent": _get_user_agent(),
    }
    merged_headers = {**default_headers, **_auth_headers(), **(headers or {})}
    resp = requests.request(
        method=method.upper(),
        url=url,
        headers=merged_headers,
        timeout=HTTP_TIMEOUT if timeout is None else timeout,
        **kwargs,
    )

    if raise_known_errors:
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
