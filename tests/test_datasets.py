from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch

from datacollective.datasets import (
    _resolve_dataset_id,
    resolve_download_dir,
)


def test_resolve_dataset_id_returns_canonical_id(monkeypatch: MonkeyPatch) -> None:
    def fake_get_dataset_details(dataset_id: str) -> dict[str, str]:
        assert dataset_id == "dataset-slug"
        return {"id": "dataset-id"}

    monkeypatch.setattr(
        "datacollective.datasets.get_dataset_details",
        fake_get_dataset_details,
    )

    assert _resolve_dataset_id("dataset-slug") == "dataset-id"


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
