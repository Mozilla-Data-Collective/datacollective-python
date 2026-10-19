from pathlib import Path

import logging
from typing import Any

import pytest

from datacollective.download import (
    DownloadPlan,
    _download_sample,
    _get_checksum_filepath,
    _determine_resume_state,
    _resolve_download_dir,
    _get_download_plan,
    _execute_download_plan,
)


def test_resolve_download_dir_prefers_argument(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("MDC_DOWNLOAD_PATH", raising=False)
    custom_dir = tmp_path / "custom"
    resolved = _resolve_download_dir(str(custom_dir))

    assert resolved == custom_dir
    assert custom_dir.exists()


def test_resolve_download_dir_uses_env_default(tmp_path: Path, monkeypatch) -> None:
    env_dir = tmp_path / "env"
    monkeypatch.setenv("MDC_DOWNLOAD_PATH", str(env_dir))
    resolved = _resolve_download_dir(None)

    assert resolved == env_dir
    assert env_dir.exists()


def _make_download_plan(
    tmp_path: Path, checksum: str = "expected_checksum"
) -> DownloadPlan:
    """Helper to create a DownloadPlan for testing."""
    target_filepath = tmp_path / "dataset.tar.gz"
    return DownloadPlan(
        download_url="https://example.com/download",
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
        result = _determine_resume_state(plan)

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
        result = _determine_resume_state(plan)

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
        result = _determine_resume_state(plan)

    assert result is None
    assert not plan.tmp_filepath.exists()
    assert "Partial download found without checksum file" in caplog.text


def test_case4_checksum_exists_no_part_cleans_up_returns_none(tmp_path: Path) -> None:
    """Case 4: .checksum exists but no .part -> start fresh (orphaned checksum)."""
    plan = _make_download_plan(tmp_path)

    # Create only .checksum file
    plan.checksum_filepath.write_text("orphan_checksum")

    result = _determine_resume_state(plan)

    assert result is None
    assert not plan.checksum_filepath.exists()


def test_case5_neither_checksum_nor_part_exist_returns_none(tmp_path: Path) -> None:
    """Case 5: Neither .checksum nor .part exist -> start fresh."""
    plan = _make_download_plan(tmp_path)

    result = _determine_resume_state(plan)

    assert result is None


def test_resume_preserves_part_file_content(tmp_path: Path) -> None:
    """When resuming, the .part file should be preserved with its content."""
    plan = _make_download_plan(tmp_path, checksum="test_checksum")

    # Create .part file with specific content
    partial_content = "first 500 bytes of download"
    plan.tmp_filepath.write_text(partial_content)

    # Create matching .checksum file
    plan.checksum_filepath.write_text("test_checksum")

    result = _determine_resume_state(plan)

    # Part file should still exist with same content
    assert result == "test_checksum"
    assert plan.tmp_filepath.exists()
    assert plan.tmp_filepath.read_text() == partial_content


def test_handles_empty_checksum_file(tmp_path: Path) -> None:
    """Empty .checksum file should be treated as no checksum."""
    plan = _make_download_plan(tmp_path, checksum="expected")

    plan.tmp_filepath.write_text("partial")
    plan.checksum_filepath.write_text("")  # Empty checksum file

    result = _determine_resume_state(plan)

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
        "datacollective.download._resolve_download_dir", fake_resolve_download_dir
    )
    monkeypatch.setattr(
        "datacollective.download._send_api_request", fake_send_api_request
    )

    _get_download_plan(
        "dataset-id",
        tmp_path,
        download_source="load_dataset",
    )

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
        "datacollective.download._send_api_request", fake_send_api_request
    )

    _execute_download_plan(
        plan,
        resume_download_checksum=None,
        show_progress=False,
        download_source="download_dataset",
    )

    assert plan.tmp_filepath.read_bytes() == b"abcdef"
    assert captured["method"] == "GET"
    assert captured["url"] == plan.download_url
    assert captured["source_function"] == "download_dataset"
    assert captured["include_auth_headers"] is False


"""
Sample downloads
"""


class _FakeSampleApi:
    """Stands in for `_send_api_request`: answers the sample-download POST with *payload*
    and the presigned GET with *content*, recording every call."""

    def __init__(self, payload: dict[str, Any], content: bytes = b"sample") -> None:
        self.payload = payload
        self.content = content
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        api = self

        class FakeResponse:
            def json(self) -> dict[str, Any]:
                return api.payload

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                return None

            def iter_content(self, chunk_size: int):
                yield api.content

        return FakeResponse()


def _sample_payload(filename: str = "sample.tar.gz") -> dict[str, Any]:
    return {
        "downloadUrl": "https://example.com/sample",
        "filename": filename,
        "sizeBytes": "6",
        "contentType": "application/gzip",
        "expiresAt": "2026-08-27T12:00:00.000Z",
    }


def test_download_sample_saves_under_samples_dir(tmp_path: Path, monkeypatch) -> None:
    api = _FakeSampleApi(_sample_payload())
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    result = _download_sample(
        "ds",
        str(tmp_path),
        show_progress=False,
        overwrite_existing=False,
        download_source="download_sample",
    )

    assert result == tmp_path / "samples" / "ds" / "sample.tar.gz"
    assert result.read_bytes() == b"sample"
    assert sorted(p.name for p in result.parent.iterdir()) == ["sample.tar.gz"]

    post, get = api.calls
    assert post["method"] == "POST"
    assert post["url"].endswith("/datasets/ds/sample-download")
    assert post["source_function"] == "download_sample"
    assert post.get("include_auth_headers", True) is True
    assert get["method"] == "GET"
    assert get["url"] == "https://example.com/sample"
    assert get["include_auth_headers"] is False


def test_download_sample_skips_download_when_already_downloaded(
    tmp_path: Path, monkeypatch
) -> None:
    existing = tmp_path / "samples" / "ds" / "sample.tar.gz"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"cached")
    api = _FakeSampleApi(_sample_payload())
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    result = _download_sample(
        "ds", str(tmp_path), show_progress=False, overwrite_existing=False
    )

    assert result == existing
    assert result.read_bytes() == b"cached"
    assert [call["method"] for call in api.calls] == ["POST"]  # no GET of the file


def test_download_sample_fetches_replaced_sample_without_overwrite(
    tmp_path: Path, monkeypatch
) -> None:
    """The existence check is on the exact filename the API returns, so an earlier sample
    with another name does not mask a sample the publisher has since replaced."""
    sample_dir = tmp_path / "samples" / "ds"
    sample_dir.mkdir(parents=True)
    (sample_dir / "old-sample.tar.gz").write_bytes(b"old")
    (sample_dir / "notes.txt").write_bytes(b"mine")
    api = _FakeSampleApi(_sample_payload("new-sample.tar.gz"))
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    result = _download_sample(
        "ds", str(tmp_path), show_progress=False, overwrite_existing=False
    )

    assert result == sample_dir / "new-sample.tar.gz"
    assert result.read_bytes() == b"sample"
    assert (sample_dir / "old-sample.tar.gz").read_bytes() == b"old"
    assert (sample_dir / "notes.txt").read_bytes() == b"mine"


def test_download_sample_restarts_interrupted_download(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    partial = tmp_path / "samples" / "ds" / "sample.tar.gz.part"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"stale")
    api = _FakeSampleApi(_sample_payload())
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    with caplog.at_level(logging.INFO, logger="datacollective.download"):
        result = _download_sample(
            "ds", str(tmp_path), show_progress=False, overwrite_existing=False
        )

    assert result.read_bytes() == b"sample"
    assert not partial.exists()
    assert "Samples cannot be resumed" in caplog.text


def test_download_sample_overwrite_replaces_existing_sample(
    tmp_path: Path, monkeypatch
) -> None:
    sample_dir = tmp_path / "samples" / "ds"
    sample_dir.mkdir(parents=True)
    (sample_dir / "sample.tar.gz").write_bytes(b"old")
    (sample_dir / "notes.txt").write_bytes(b"mine")
    api = _FakeSampleApi(_sample_payload())
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    result = _download_sample(
        "ds", str(tmp_path), show_progress=False, overwrite_existing=True
    )

    assert result == sample_dir / "sample.tar.gz"
    assert result.read_bytes() == b"sample"
    assert [call["method"] for call in api.calls] == ["POST", "GET"]
    # Only the sample itself is replaced; other files in the directory are left alone.
    assert (sample_dir / "notes.txt").read_bytes() == b"mine"


@pytest.mark.parametrize(
    "filename", ["../escape.tar.gz", "nested/sample.tar.gz", "", ".hidden.tar.gz"]
)
def test_download_sample_rejects_unsafe_filename(
    tmp_path: Path, monkeypatch, filename: str
) -> None:
    api = _FakeSampleApi(_sample_payload(filename))
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    with pytest.raises(RuntimeError, match="Unexpected sample filename"):
        _download_sample(
            "ds", str(tmp_path), show_progress=False, overwrite_existing=False
        )

    assert len(api.calls) == 1  # nothing downloaded


def test_download_sample_refused_leaves_no_directory(
    tmp_path: Path, monkeypatch
) -> None:
    def refuse(**kwargs: Any) -> Any:
        raise PermissionError("You must agree to the terms of use")

    monkeypatch.setattr("datacollective.download._send_api_request", refuse)

    with pytest.raises(PermissionError):
        _download_sample(
            "ds", str(tmp_path), show_progress=False, overwrite_existing=False
        )

    assert not (tmp_path / "samples").exists()
