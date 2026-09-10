import pytest

from datacollective import (
    DatasetFilters,
    DatasetList,
    DatasetDetails,
    get_dataset_details,
    list_dataset_filters,
    list_datasets,
)
from tests.e2e.conftest import skip_if_rate_limited


@pytest.fixture
def live_public_api_env(live_api_env: None, monkeypatch) -> None:
    """GET /datasets and GET /datasets/filters are public: run them with no API key."""
    monkeypatch.delenv("MDC_API_KEY", raising=False)


def test_list_dataset_filters_live_api(live_public_api_env: None) -> None:
    """NOTE: This test calls a live MDC API endpoint (dev)."""
    filters = None

    try:
        filters = list_dataset_filters()
    except Exception as exc:
        skip_if_rate_limited(exc)

    assert isinstance(filters, DatasetFilters)
    # The task vocabulary is fixed on the platform and always populated.
    assert filters.tasks
    assert all(isinstance(task, str) and task for task in filters.tasks)
    assert isinstance(filters.locales, list)
    assert isinstance(filters.licenses, list)
    assert isinstance(filters.formats, list)


def test_list_datasets_live_api(live_public_api_env: None) -> None:
    """NOTE: This test calls a live MDC API endpoint (dev)."""
    page = None

    try:
        page = list_datasets(results_per_page=5, page_number=1, sort="newest")
    except Exception as exc:
        skip_if_rate_limited(exc)

    assert isinstance(page, DatasetList)
    assert page.total >= len(page)
    assert len(page) <= 5
    for item in page:
        assert isinstance(item, DatasetDetails)
        assert item.id.strip()
        assert item.slug and item.slug.strip()
        assert item.name and item.name.strip()


def test_list_datasets_filter_roundtrip_live_api(live_public_api_env: None) -> None:
    """Filter by the first advertised task and check every result carries it.

    NOTE: This test calls a live MDC API endpoint (dev).
    """
    page = None

    try:
        filters = list_dataset_filters()
        assert filters.tasks
        task = filters.tasks[0]
        page = list_datasets(task=task, results_per_page=10)
    except Exception as exc:
        skip_if_rate_limited(exc)

    assert page is not None
    assert all(item.task == task for item in page)


def test_list_datasets_matches_details_live_api(
    live_public_api_env: None, dev_dataset_id: str
) -> None:
    """A catalog entry must resolve to the same dataset via `get_dataset_details`.

    NOTE: This test calls a live MDC API endpoint (dev).
    """
    page = None

    try:
        details = get_dataset_details(dev_dataset_id)
        assert details.name
        page = list_datasets(details.name, results_per_page=50)
    except Exception as exc:
        skip_if_rate_limited(exc)

    assert page is not None
    matches = [item for item in page if item.id == dev_dataset_id]
    assert matches, f"Dataset {dev_dataset_id} not found when searching its own name"
    assert matches[0].slug == details.slug
    assert matches[0].filename == details.filename
