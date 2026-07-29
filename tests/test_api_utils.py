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
