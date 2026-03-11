import re

from datacollective.api_utils import _get_user_agent


def test_get_user_agent_omits_download_source_by_default() -> None:
    user_agent = _get_user_agent()

    assert "datacollective-python/" in user_agent
    assert "mdc-download-source/" not in user_agent
    assert re.search(r"\(Python .*; .*\)", user_agent)


def test_get_user_agent_appends_download_source_token() -> None:
    user_agent = _get_user_agent(source_function="load_dataset")

    assert user_agent.endswith("source function: load_dataset")
