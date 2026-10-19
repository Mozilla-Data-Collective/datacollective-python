import re

import datacollective.api_utils as api_utils


def test_get_user_agent_omits_download_source_by_default() -> None:
    user_agent = api_utils._get_user_agent()

    assert "datacollective-python/" in user_agent
    assert "mdc-download-source/" not in user_agent
    assert re.search(r"\(Python .*; .*\)", user_agent)


def test_get_user_agent_appends_download_source_token() -> None:
    user_agent = api_utils._get_user_agent(source_function="load_dataset")

    assert user_agent.endswith("source function: load_dataset")


def test_get_api_url_uses_new_default(monkeypatch) -> None:
    monkeypatch.delenv(api_utils.ENV_API_URL, raising=False)

    assert api_utils._get_api_url() == api_utils.DEFAULT_API_URL


def test_get_api_url_preserves_custom_url(monkeypatch) -> None:
    custom_url = "https://api.example.test/custom"
    monkeypatch.setenv(api_utils.ENV_API_URL, custom_url)

    assert api_utils._get_api_url() == custom_url


def test_redact_sensitive_masks_urls_tokens_and_emails() -> None:
    payload = {
        "downloadUrl": "https://signed.example.com/abc?sig=secret",
        "presignedUrl": "https://signed.example.com/part",
        "downloadToken": "tok_123",
        "filename": "dataset.tar.gz",
        "sizeBytes": 1000,
        "parts": [{"partNumber": 1, "url": "https://signed.example.com/1"}],
    }

    redacted = api_utils._redact_sensitive(payload)

    assert redacted["downloadUrl"] == api_utils._REDACTED
    assert redacted["presignedUrl"] == api_utils._REDACTED
    assert redacted["downloadToken"] == api_utils._REDACTED
    # Non-sensitive fields are preserved, including nested ones.
    assert redacted["filename"] == "dataset.tar.gz"
    assert redacted["sizeBytes"] == 1000
    assert redacted["parts"][0]["partNumber"] == 1
    assert redacted["parts"][0]["url"] == api_utils._REDACTED


def test_redact_sensitive_passes_through_non_dict_values() -> None:
    assert api_utils._redact_sensitive(None) is None
    assert api_utils._redact_sensitive("plain") == "plain"


def test_redact_url_hides_query_string_only() -> None:
    signed = (
        "https://storage.example.com/d/x.tar.gz?X-Amz-Signature=abc&X-Amz-Expires=60"
    )
    assert api_utils._redact_url(signed) == (
        f"https://storage.example.com/d/x.tar.gz?{api_utils._REDACTED}"
    )
    plain = "https://mozilladatacollective.com/api/datasets/abc"
    assert api_utils._redact_url(plain) == plain


def test_send_api_request_never_logs_or_raises_presigned_query(
    monkeypatch, caplog
) -> None:
    import logging

    import requests

    signed = "https://storage.example.com/d/x.tar.gz?X-Amz-Signature=secret123"
    resp = requests.Response()
    resp.status_code = 403
    resp._content = b"{}"
    monkeypatch.setattr(api_utils.requests, "request", lambda **kwargs: resp)

    with caplog.at_level(logging.DEBUG, logger=api_utils.logger.name):
        try:
            api_utils._send_api_request("GET", signed, include_auth_headers=False)
        except PermissionError as exc:
            assert "secret123" not in str(exc)
            assert api_utils._REDACTED in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected PermissionError")

    assert "secret123" not in caplog.text
    assert api_utils._REDACTED in caplog.text
