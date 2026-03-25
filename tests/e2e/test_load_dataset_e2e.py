import pandas as pd
from pathlib import Path

from datacollective import load_dataset
from tests.e2e.conftest import skip_if_rate_limited


def test_load_dataset_live_api(
    live_download_dir: Path,
    dev_dataset_id: str,
) -> None:
    """NOTE: This test calls a live MDC API endpoint (dev)."""

    df = None
    try:
        df = load_dataset(
            dev_dataset_id,
            download_directory=str(live_download_dir),
            show_progress=False,
            overwrite_existing=True,
        )
    except Exception as exc:  # noqa: BLE001
        skip_if_rate_limited(exc)

    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df.columns) > 0
