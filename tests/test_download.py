from pathlib import Path

import pytest

from datacollective.download import (
    DownloadPlan,
    _get_checksum_filepath,
    determine_resume_state,
)


def _make_download_plan(
    tmp_path: Path, checksum: str = "expected_checksum"
) -> DownloadPlan:
    """Helper to create a DownloadPlan for testing."""
    target_filepath = tmp_path / "dataset.tar.gz"
    return DownloadPlan(
        download_url="https://example.com/download",
        filename="dataset.tar.gz",
        target_filepath=target_filepath,
        tmp_filepath=target_filepath.with_name(target_filepath.name + ".part"),
        size_bytes=1000,
        checksum=checksum,
        checksum_filepath=_get_checksum_filepath(target_filepath),
    )


def test_case1_checksum_and_part_exist_checksum_matches_returns_checksum(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Case 1: .checksum and .part exist, checksum matches -> resume download."""
    plan = _make_download_plan(tmp_path, checksum="matching_checksum")

    # Create .part and .checksum files
    plan.tmp_filepath.write_text("partial data")
    plan.checksum_filepath.write_text("matching_checksum")

    result = determine_resume_state(plan)

    assert result == "matching_checksum"
    captured = capsys.readouterr()
    assert "Resuming previously interrupted download" in captured.out


def test_case2_checksum_and_part_exist_checksum_mismatch_cleans_up_returns_none(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Case 2: .checksum and .part exist, checksum does NOT match -> start fresh."""
    plan = _make_download_plan(tmp_path, checksum="new_checksum")

    # Create .part and .checksum files with old checksum
    plan.tmp_filepath.write_text("partial data")
    plan.checksum_filepath.write_text("old_checksum")

    result = determine_resume_state(plan)

    assert result is None
    assert not plan.tmp_filepath.exists()
    assert not plan.checksum_filepath.exists()
    captured = capsys.readouterr()
    assert "Dataset has been updated" in captured.out


def test_case3_part_exists_no_checksum_cleans_up_returns_none(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Case 3: .part exists but no .checksum -> start fresh (cannot safely resume)."""
    plan = _make_download_plan(tmp_path)

    # Create only .part file
    plan.tmp_filepath.write_text("partial data")

    result = determine_resume_state(plan)

    assert result is None
    assert not plan.tmp_filepath.exists()
    captured = capsys.readouterr()
    assert "Partial download found without checksum file" in captured.out


def test_case4_checksum_exists_no_part_cleans_up_returns_none(tmp_path: Path) -> None:
    """Case 4: .checksum exists but no .part -> start fresh (orphaned checksum)."""
    plan = _make_download_plan(tmp_path)

    # Create only .checksum file
    plan.checksum_filepath.write_text("orphan_checksum")

    result = determine_resume_state(plan)

    assert result is None
    assert not plan.checksum_filepath.exists()


def test_case5_neither_checksum_nor_part_exist_returns_none(tmp_path: Path) -> None:
    """Case 5: Neither .checksum nor .part exist -> start fresh."""
    plan = _make_download_plan(tmp_path)

    result = determine_resume_state(plan)

    assert result is None


def test_resume_preserves_part_file_content(tmp_path: Path) -> None:
    """When resuming, the .part file should be preserved with its content."""
    plan = _make_download_plan(tmp_path, checksum="test_checksum")

    # Create .part file with specific content
    partial_content = b"first 500 bytes of download"
    plan.tmp_filepath.write_bytes(partial_content)

    # Create matching .checksum file
    plan.checksum_filepath.write_text("test_checksum")

    result = determine_resume_state(plan)

    # Part file should still exist with same content
    assert result == "test_checksum"
    assert plan.tmp_filepath.exists()
    assert plan.tmp_filepath.read_bytes() == partial_content


def test_handles_empty_checksum_file(tmp_path: Path) -> None:
    """Empty .checksum file should be treated as no checksum."""
    plan = _make_download_plan(tmp_path, checksum="expected")

    plan.tmp_filepath.write_text("partial")
    plan.checksum_filepath.write_text("")  # Empty checksum file

    result = determine_resume_state(plan)

    # Empty stored_checksum is falsy, so it won't match
    assert result is None
