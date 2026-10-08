import tarfile
from pathlib import Path

import pytest

from datacollective.archive_utils import _extract_archive


def _make_tar_gz(tmp_path: Path, name: str = "sample.tar.gz") -> Path:
    content = tmp_path / "data.txt"
    content.write_text("hello")
    archive = tmp_path / name
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(content, arcname="data.txt")
    return archive


def test_extract_archive_extracts_into_directory_named_after_archive(
    tmp_path: Path,
) -> None:
    archive = _make_tar_gz(tmp_path)
    dest = tmp_path / "out"

    target = _extract_archive(archive, dest, overwrite_extracted=False)

    assert target == dest / "sample"
    assert (target / "data.txt").read_text() == "hello"


def test_extract_archive_accepts_tgz(tmp_path: Path) -> None:
    archive = _make_tar_gz(tmp_path, name="sample.tgz")
    dest = tmp_path / "out"

    target = _extract_archive(archive, dest, overwrite_extracted=False)

    assert target == dest / "sample"
    assert (target / "data.txt").read_text() == "hello"


def test_extract_archive_skips_existing_directory(tmp_path: Path) -> None:
    archive = _make_tar_gz(tmp_path)
    existing = tmp_path / "out" / "sample"
    existing.mkdir(parents=True)

    target = _extract_archive(archive, tmp_path / "out", overwrite_extracted=False)

    assert target == existing
    assert not (target / "data.txt").exists()


def test_extract_archive_rejects_non_tar_gz(tmp_path: Path) -> None:
    archive = tmp_path / "sample.zip"
    archive.write_bytes(b"")

    with pytest.raises(ValueError, match=r"\.tar\.gz or \.tgz"):
        _extract_archive(archive, tmp_path, overwrite_extracted=False)
