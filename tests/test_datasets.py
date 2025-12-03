from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch

from datacollective.datasets import _resolve_download_dir, _strip_archive_suffix


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
