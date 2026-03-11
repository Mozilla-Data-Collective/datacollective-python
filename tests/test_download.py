from pathlib import Path

import logging
from typing import Any

from datacollective.download import (
    DownloadPlan,
    _get_checksum_filepath,
    determine_resume_state,
    execute_download_plan,
    get_download_plan,
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
    tmp_path: Path, caplog
) -> None:
    """Case 1: .checksum and .part exist, checksum matches -> resume download."""
    plan = _make_download_plan(tmp_path, checksum="matching_checksum")

    # Create .part and .checksum files
    plan.tmp_filepath.write_text("partial data")
    plan.checksum_filepath.write_text("matching_checksum")

    with caplog.at_level(logging.INFO, logger="datacollective.download"):
        result = determine_resume_state(plan)

    assert result == "matching_checksum"
    assert "Resuming previously interrupted download" in caplog.text


def test_case2_checksum_and_part_exist_checksum_mismatch_cleans_up_returns_none(
    tmp_path: Path, caplog
) -> None:
    """Case 2: .checksum and .part exist, checksum does NOT match -> start fresh."""
    plan = _make_download_plan(tmp_path, checksum="new_checksum")

    # Create .part and .checksum files with old checksum
    plan.tmp_filepath.write_text("partial data")
    plan.checksum_filepath.write_text("old_checksum")

    with caplog.at_level(logging.INFO, logger="datacollective.download"):
        result = determine_resume_state(plan)

    assert result is None
    assert not plan.tmp_filepath.exists()
    assert not plan.checksum_filepath.exists()
    assert "Dataset has been updated" in caplog.text


def test_case3_part_exists_no_checksum_cleans_up_returns_none(
    tmp_path: Path, caplog
) -> None:
    """Case 3: .part exists but no .checksum -> start fresh (cannot safely resume)."""
    plan = _make_download_plan(tmp_path)

    # Create only .part file
    plan.tmp_filepath.write_text("partial data")

    with caplog.at_level(logging.WARNING, logger="datacollective.download"):
        result = determine_resume_state(plan)

    assert result is None
    assert not plan.tmp_filepath.exists()
    assert "Partial download found without checksum file" in caplog.text


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
    partial_content = "first 500 bytes of download"
    plan.tmp_filepath.write_text(partial_content)

    # Create matching .checksum file
    plan.checksum_filepath.write_text("test_checksum")

    result = determine_resume_state(plan)

    # Part file should still exist with same content
    assert result == "test_checksum"
    assert plan.tmp_filepath.exists()
    assert plan.tmp_filepath.read_text() == partial_content


def test_handles_empty_checksum_file(tmp_path: Path) -> None:
    """Empty .checksum file should be treated as no checksum."""
    plan = _make_download_plan(tmp_path, checksum="expected")

    plan.tmp_filepath.write_text("partial")
    plan.checksum_filepath.write_text("")  # Empty checksum file

    result = determine_resume_state(plan)

    # Empty stored_checksum is falsy, so it won't match
    assert result is None


def test_get_download_plan_forwards_download_source(
    tmp_path: Path, monkeypatch
) -> None:
    captured: dict[str, Any] = {}

    class FakeResponse:
        def json(self) -> dict[str, Any]:
            return {
                "downloadUrl": "https://example.com/download",
                "filename": "dataset.tar.gz",
                "sizeBytes": 1000,
                "checksum": "abc123",
            }

    def fake_resolve_download_dir(download_directory: str | None) -> Path:
        assert download_directory == str(tmp_path)
        return tmp_path

    def fake_send_api_request(**kwargs: Any) -> FakeResponse:
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(
        "datacollective.download.resolve_download_dir", fake_resolve_download_dir
    )
    monkeypatch.setattr(
        "datacollective.download.send_api_request", fake_send_api_request
    )

    plan = get_download_plan(
        "dataset-id",
        str(tmp_path),
        download_source="load_dataset",
    )

    assert plan.filename == "dataset.tar.gz"
    assert captured["method"] == "POST"
    assert captured["source_function"] == "load_dataset"


def test_execute_download_plan_forwards_download_source(
    tmp_path: Path, monkeypatch
) -> None:
    captured: dict[str, Any] = {}
    plan = _make_download_plan(tmp_path)

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def iter_content(self, chunk_size: int):
            assert chunk_size == 1 << 16
            yield b"abc"
            yield b"def"

    def fake_send_api_request(**kwargs: Any) -> FakeResponse:
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(
        "datacollective.download.send_api_request", fake_send_api_request
    )

    execute_download_plan(
        plan,
        resume_download_checksum=None,
        show_progress=False,
        download_source="save_dataset_to_disk",
    )

    assert plan.tmp_filepath.read_bytes() == b"abcdef"
    assert captured["method"] == "GET"
    assert captured["url"] == plan.download_url
    assert captured["source_function"] == "save_dataset_to_disk"
    assert captured["include_auth_headers"] is False
