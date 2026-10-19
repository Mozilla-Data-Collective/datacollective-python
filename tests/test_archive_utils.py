import tarfile
from pathlib import Path

import pytest

from datacollective.archive_utils import _extract_archive, _extracted_dir_name


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


def test_extract_archive_interrupted_leaves_no_finished_directory(
    tmp_path: Path,
) -> None:
    archive = _make_tar_gz(tmp_path)
    good = archive.read_bytes()
    archive.write_bytes(good[: len(good) // 2])  # truncated, as if cut off mid-download
    dest = tmp_path / "out"

    with pytest.raises((tarfile.TarError, EOFError)):
        _extract_archive(archive, dest, overwrite_extracted=False)

    # Nothing that looks finished, so the next call extracts rather than skipping
    assert not (dest / "sample").exists()

    archive.write_bytes(good)
    target = _extract_archive(archive, dest, overwrite_extracted=False)

    assert (target / "data.txt").read_text() == "hello"
    assert sorted(p.name for p in dest.iterdir()) == ["sample"]


def test_extract_archive_overwrite_replaces_directory(tmp_path: Path) -> None:
    archive = _make_tar_gz(tmp_path)
    stale = tmp_path / "out" / "sample"
    stale.mkdir(parents=True)
    (stale / "old.txt").write_text("stale")

    target = _extract_archive(archive, tmp_path / "out", overwrite_extracted=True)

    assert sorted(p.name for p in target.iterdir()) == ["data.txt"]


@pytest.mark.parametrize("overwrite_extracted", [False, True])
@pytest.mark.parametrize("stray_name", ["sample", "sample.partial"])
def test_extract_archive_refuses_to_touch_stray_file(
    tmp_path: Path, stray_name: str, overwrite_extracted: bool
) -> None:
    archive = _make_tar_gz(tmp_path)
    dest = tmp_path / "out"
    dest.mkdir()
    stray = dest / stray_name
    stray.write_text("mine")

    with pytest.raises(FileExistsError, match="is not a directory"):
        _extract_archive(archive, dest, overwrite_extracted=overwrite_extracted)

    # The stray file is left exactly as it was and nothing else was created.
    assert stray.read_text() == "mine"
    assert sorted(p.name for p in dest.iterdir()) == [stray_name]


@pytest.mark.parametrize(
    ("archive_name", "expected"),
    [
        ("dataset.tar.gz", "dataset"),
        ("dataset.tgz", "dataset"),
        ("dataset.zip", None),
        (".tar.gz", None),
    ],
)
def test_extracted_dir_name(archive_name: str, expected: str | None) -> None:
    assert _extracted_dir_name(archive_name) == expected
