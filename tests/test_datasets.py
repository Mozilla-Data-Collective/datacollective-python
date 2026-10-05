from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from datacollective import datasets
from datacollective.models import DatasetDetails
from datacollective.download import _resolve_download_dir


def test_resolve_download_dir_prefers_argument(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.delenv("MDC_DOWNLOAD_PATH", raising=False)
    custom_dir = tmp_path / "custom"
    resolved = _resolve_download_dir(str(custom_dir))

    assert resolved == custom_dir
    assert custom_dir.exists()


def test_resolve_download_dir_uses_env_default(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    env_dir = tmp_path / "env"
    monkeypatch.setenv("MDC_DOWNLOAD_PATH", str(env_dir))
    resolved = _resolve_download_dir(None)

    assert resolved == env_dir
    assert env_dir.exists()


def _capture_downloads(monkeypatch: MonkeyPatch) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []

    def fake_download_dataset(**kwargs: object) -> Path:
        calls.append(kwargs)
        return Path("archive.tar.gz")

    monkeypatch.setattr(
        datasets,
        "get_dataset_details",
        lambda dataset_id: DatasetDetails(id=dataset_id, filename="archive.tar.gz"),
    )
    monkeypatch.setattr(datasets, "_download_dataset", fake_download_dataset)
    return calls


def test_download_dataset_reports_its_own_source(monkeypatch: MonkeyPatch) -> None:
    calls = _capture_downloads(monkeypatch)

    datasets.download_dataset("ds")

    assert calls[0]["download_source"] == "download_dataset"


def test_save_dataset_to_disk_warns_and_reports_its_own_source(
    monkeypatch: MonkeyPatch,
) -> None:
    calls = _capture_downloads(monkeypatch)

    with pytest.warns(DeprecationWarning, match="download_dataset"):
        result = datasets.save_dataset_to_disk("ds", download_directory="dir")

    assert result == Path("archive.tar.gz")
    assert calls == [
        {
            "dataset_id": "ds",
            "archive_filename": "archive.tar.gz",
            "download_directory": "dir",
            "show_progress": True,
            "overwrite_existing": False,
            "download_source": "save_dataset_to_disk",
        }
    ]
