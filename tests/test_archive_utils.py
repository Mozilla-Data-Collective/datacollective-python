from pathlib import Path

from datacollective.archive_utils import _strip_archive_suffix


def test_strip_archive_suffix_removes_known_extensions(tmp_path: Path) -> None:
    tar_path = tmp_path / "sample.tar.gz"
    zip_path = tmp_path / "sample.zip"

    assert _strip_archive_suffix(tar_path).name == "sample"
    assert _strip_archive_suffix(zip_path).name == "sample"
