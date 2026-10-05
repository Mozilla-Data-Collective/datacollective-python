from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from datacollective import datasets
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


def test_save_dataset_to_disk_warns_and_delegates(monkeypatch: MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_download_dataset(**kwargs: object) -> Path:
        calls.append(kwargs)
        return Path("archive.tar.gz")

    monkeypatch.setattr(datasets, "download_dataset", fake_download_dataset)
    with pytest.warns(DeprecationWarning, match="download_dataset"):
        result = datasets.save_dataset_to_disk("ds", download_directory="dir")

    assert result == Path("archive.tar.gz")
    assert calls == [
        {
            "dataset_id": "ds",
            "download_directory": "dir",
            "show_progress": True,
            "overwrite_existing": False,
            "enable_logging": False,
        }
    ]
