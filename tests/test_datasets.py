from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

import datacollective
from datacollective.datasets import (
    _strip_archive_suffix,
    resolve_download_dir,
    save_dataset_to_disk,
)


def test_strip_archive_suffix_removes_known_extensions(tmp_path: Path) -> None:
    tar_path = tmp_path / "sample.tar.gz"
    zip_path = tmp_path / "sample.zip"

    assert _strip_archive_suffix(tar_path).name == "sample"
    assert _strip_archive_suffix(zip_path).name == "sample"


def test_resolve_download_dir_prefers_argument(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.delenv("MDC_DOWNLOAD_PATH", raising=False)
    custom_dir = tmp_path / "custom"
    resolved = resolve_download_dir(str(custom_dir))

    assert resolved == custom_dir
    assert custom_dir.exists()


def test_resolve_download_dir_uses_env_default(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    env_dir = tmp_path / "env"
    monkeypatch.setenv("MDC_DOWNLOAD_PATH", str(env_dir))
    resolved = resolve_download_dir(None)

    assert resolved == env_dir
    assert env_dir.exists()


def test_download_dataset_is_exported_from_package() -> None:
    assert datacollective.download_dataset is datacollective.datasets.download_dataset


def test_save_dataset_to_disk_warns_and_delegates(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    expected_path = tmp_path / "dataset.tar.gz"
    captured_kwargs: dict[str, object] = {}

    def fake_download_dataset(**kwargs: object) -> Path:
        captured_kwargs.update(kwargs)
        return expected_path

    monkeypatch.setattr(
        datacollective.datasets, "download_dataset", fake_download_dataset
    )

    with pytest.warns(DeprecationWarning, match="download_dataset"):
        result = save_dataset_to_disk(
            "dataset-id",
            download_directory=str(tmp_path),
            show_progress=False,
            overwrite_existing=True,
        )

    assert result == expected_path
    assert captured_kwargs == {
        "dataset_id": "dataset-id",
        "download_directory": str(tmp_path),
        "show_progress": False,
        "overwrite_existing": True,
    }
