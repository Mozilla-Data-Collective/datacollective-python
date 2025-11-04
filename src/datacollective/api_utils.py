import os

DEFAULT_API_URL = "https://datacollective.mozillafoundation.org/api"
ENV_API_KEY = "MDC_API_KEY"
ENV_API_URL = "MDC_API_URL"
ENV_DOWNLOAD_PATH = "MDC_DOWNLOAD_PATH"
HTTP_TIMEOUT = (10, 60)  # (connect, read)

RATE_LIMIT_ERROR = "Rate limit exceeded. Please try again later."


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
