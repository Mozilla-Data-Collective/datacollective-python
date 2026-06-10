import logging
import re
import warnings

import datacollective.api_utils as api_utils


def test_get_user_agent_omits_download_source_by_default() -> None:
    user_agent = api_utils._get_user_agent()

    assert "datacollective-python/" in user_agent
    assert "mdc-download-source/" not in user_agent
    assert re.search(r"\(Python .*; .*\)", user_agent)


def test_get_user_agent_appends_download_source_token() -> None:
    user_agent = api_utils._get_user_agent(source_function="load_dataset")

    assert user_agent.endswith("source function: load_dataset")


def test_get_api_url_uses_new_default_without_warning(monkeypatch) -> None:
    monkeypatch.delenv(api_utils.ENV_API_URL, raising=False)
    monkeypatch.setattr(api_utils, "_LEGACY_API_URL_NOTICE_EMITTED", False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        api_url = api_utils._get_api_url()

    assert api_url == api_utils.DEFAULT_API_URL
    assert not caught


def test_get_api_url_rewrites_legacy_env_and_warns_once(monkeypatch, caplog) -> None:
    monkeypatch.setenv(api_utils.ENV_API_URL, api_utils.LEGACY_API_URL)
    monkeypatch.setattr(api_utils, "_LEGACY_API_URL_NOTICE_EMITTED", False)

    with (
        warnings.catch_warnings(record=True) as caught,
        caplog.at_level(logging.WARNING, logger="datacollective.api_utils"),
    ):
        warnings.simplefilter("always")
        first_url = api_utils._get_api_url()
        second_url = api_utils._get_api_url()

    assert first_url == api_utils.DEFAULT_API_URL
    assert second_url == api_utils.DEFAULT_API_URL
    assert len(caught) == 1

    warning_message = str(caught[0].message)
    assert api_utils.LEGACY_API_URL in warning_message
    assert api_utils.DEFAULT_API_URL in warning_message
    assert "set to the legacy API URL" in warning_message

    logged_messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "datacollective.api_utils"
    ]
    assert logged_messages == [warning_message]


def test_get_api_url_preserves_custom_url_without_warning(monkeypatch) -> None:
    custom_url = "https://api.example.test/custom"
    monkeypatch.setenv(api_utils.ENV_API_URL, custom_url)
    monkeypatch.setattr(api_utils, "_LEGACY_API_URL_NOTICE_EMITTED", False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        api_url = api_utils._get_api_url()

    assert api_url == "https://api.example.test/custom"
    assert not caught
