from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Any

import requests

DEFAULT_API_URL = "https://datacollective.mozillafoundation.org/api"
ENV_API_KEY = "MDC_API_KEY"
ENV_API_URL = "MDC_API_URL"
ENV_DOWNLOAD_PATH = "MDC_DOWNLOAD_PATH"
HTTP_TIMEOUT = (10, 60)  # (connect, read)

RATE_LIMIT_ERROR = "Rate limit exceeded. Please try again later."


def api_request(
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
    merged_headers = {**_auth_headers(), **(headers or {})}
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


def _extract_checksum_from_api_response(
    api_response: requests.Response,
) -> str | None:
    repr_digest = api_response.headers.get("Repr-Digest")
    if not repr_digest:
        return None
    _, digest, _ = repr_digest.split("=:")
    return base64.b64decode(digest).hex()


def _prepare_download_headers(
    tmp_path: Path, resume_checksum: str | None
) -> tuple[dict[str, str], int]:
    """Prepare headers for download plan and determine existing file size for download."""
    if not tmp_path.exists():  # invalid path
        return {}, 0

    if resume_checksum:
        existing_size = tmp_path.stat().st_size
        return {"Range": f"bytes={existing_size}-"}, existing_size

    tmp_path.unlink()  # remove existing file if no resume checksum supplied
    return {}, 0
