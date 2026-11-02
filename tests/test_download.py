from pathlib import Path

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import requests

from datacollective.download import (
    DEFAULT_MAX_DOWNLOAD_ATTEMPTS,
    DownloadPlan,
    _download_dataset,
    _download_sample,
    _load_stored_download_plan,
    _run_download_plan,
    _get_checksum_filepath,
    _determine_resume_state,
    _resolve_download_dir,
    _get_download_plan,
    _execute_download_plan,
)
from datacollective.errors import DownloadError


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


class _FlakyDownloadApi:
    """Stands in for `_send_api_request` on the presigned GET: serves *content* but drops the
    connection after *fail_after* bytes on each of the first *failures* calls, then honours
    the Range header on later calls. Records every call."""

    def __init__(self, content: bytes, fail_after: int, failures: int) -> None:
        self.content = content
        self.fail_after = fail_after
        self.failures = failures
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        api = self
        attempt = len(self.calls)
        range_header = (kwargs.get("extra_headers") or {}).get("Range")
        start = (
            int(range_header.removeprefix("bytes=").rstrip("-")) if range_header else 0
        )

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                return None

            def iter_content(self, chunk_size: int):
                body = api.content[start:]
                if attempt <= api.failures:
                    yield body[: api.fail_after]
                    raise requests.ConnectionError("connection reset")
                yield body

        return FakeResponse()


def test_execute_download_plan_retries_same_url_after_connection_drop(
    tmp_path: Path, monkeypatch
) -> None:
    plan = _make_download_plan(tmp_path)
    api = _FlakyDownloadApi(content=b"0123456789", fail_after=4, failures=1)
    sleeps: list[float] = []
    monkeypatch.setattr("datacollective.download._send_api_request", api)
    monkeypatch.setattr("datacollective.download.time.sleep", sleeps.append)

    _execute_download_plan(
        plan, resume_download_checksum=None, show_progress=False, download_source=None
    )

    assert plan.tmp_filepath.read_bytes() == b"0123456789"
    assert len(api.calls) == 2
    # Both attempts hit the same presigned URL; no new download session was requested.
    assert {call["url"] for call in api.calls} == {plan.download_url}
    assert {call["method"] for call in api.calls} == {"GET"}
    assert "Range" not in (api.calls[0]["extra_headers"] or {})
    assert api.calls[1]["extra_headers"]["Range"] == "bytes=4-"
    assert sleeps == [2]


def test_execute_download_plan_retry_continues_from_resumed_offset(
    tmp_path: Path, monkeypatch
) -> None:
    plan = _make_download_plan(tmp_path)
    plan.tmp_filepath.write_bytes(b"012")
    api = _FlakyDownloadApi(content=b"0123456789", fail_after=2, failures=1)
    monkeypatch.setattr("datacollective.download._send_api_request", api)
    monkeypatch.setattr("datacollective.download.time.sleep", lambda _s: None)

    _execute_download_plan(
        plan, resume_download_checksum="abc", show_progress=False, download_source=None
    )

    assert plan.tmp_filepath.read_bytes() == b"0123456789"
    assert api.calls[0]["extra_headers"]["Range"] == "bytes=3-"
    assert api.calls[1]["extra_headers"]["Range"] == "bytes=5-"


def test_execute_download_plan_gives_up_after_max_attempts(
    tmp_path: Path, monkeypatch
) -> None:
    plan = _make_download_plan(tmp_path)
    api = _FlakyDownloadApi(content=b"0123456789", fail_after=1, failures=99)
    sleeps: list[float] = []
    monkeypatch.setattr("datacollective.download._send_api_request", api)
    monkeypatch.setattr("datacollective.download.time.sleep", sleeps.append)

    with pytest.raises(DownloadError) as excinfo:
        _execute_download_plan(
            plan,
            resume_download_checksum=None,
            show_progress=False,
            download_source=None,
        )

    assert len(api.calls) == DEFAULT_MAX_DOWNLOAD_ATTEMPTS
    assert sleeps == [2, 4]
    assert isinstance(excinfo.value.__cause__, requests.ConnectionError)
    # One byte landed on disk per attempt.
    assert plan.tmp_filepath.read_bytes() == b"012"


def test_execute_download_plan_honours_max_download_attempts(
    tmp_path: Path, monkeypatch
) -> None:
    plan = _make_download_plan(tmp_path)
    api = _FlakyDownloadApi(content=b"0123456789", fail_after=1, failures=99)
    monkeypatch.setattr("datacollective.download._send_api_request", api)
    monkeypatch.setattr("datacollective.download.time.sleep", lambda _s: None)

    with pytest.raises(DownloadError):
        _execute_download_plan(
            plan,
            resume_download_checksum=None,
            show_progress=False,
            max_download_attempts=1,
            download_source=None,
        )

    assert len(api.calls) == 1


def test_execute_download_plan_rejects_zero_attempts(tmp_path: Path) -> None:
    plan = _make_download_plan(tmp_path)
    with pytest.raises(ValueError, match="max_download_attempts"):
        _execute_download_plan(
            plan,
            resume_download_checksum=None,
            show_progress=False,
            max_download_attempts=0,
            download_source=None,
        )


def test_execute_download_plan_does_not_retry_other_errors(
    tmp_path: Path, monkeypatch
) -> None:
    plan = _make_download_plan(tmp_path)
    calls: list[dict[str, Any]] = []

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def iter_content(self, chunk_size: int):
            yield b"abc"
            raise requests.HTTPError("403 Forbidden")

    def fake_send_api_request(**kwargs: Any) -> FakeResponse:
        calls.append(kwargs)
        return FakeResponse()

    monkeypatch.setattr(
        "datacollective.download._send_api_request", fake_send_api_request
    )

    with pytest.raises(DownloadError):
        _execute_download_plan(
            plan,
            resume_download_checksum=None,
            show_progress=False,
            download_source=None,
        )

    assert len(calls) == 1


"""
Resuming with the stored presigned URL
"""


class _FakeDatasetApi:
    """Stands in for `_send_api_request`: answers the download-session POST with *payload*
    and any presigned GET with *content* (honouring the Range header), recording every
    call. A URL listed in *rejected_urls* answers the GET with a PermissionError instead,
    as the storage would an expired or revoked presigned URL."""

    def __init__(
        self,
        payload: dict[str, Any],
        content: bytes,
        rejected_urls: tuple[str, ...] = (),
    ) -> None:
        self.payload = payload
        self.content = content
        self.rejected_urls = rejected_urls
        self.calls: list[dict[str, Any]] = []

    @property
    def methods(self) -> list[str]:
        return [call["method"] for call in self.calls]

    def __call__(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if kwargs["method"] == "GET" and kwargs["url"] in self.rejected_urls:
            raise PermissionError("403 Forbidden")
        api = self
        range_header = (kwargs.get("extra_headers") or {}).get("Range")
        start = (
            int(range_header.removeprefix("bytes=").rstrip("-")) if range_header else 0
        )

        class FakeResponse:
            def json(self) -> dict[str, Any]:
                return api.payload

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                return None

            def iter_content(self, chunk_size: int):
                yield api.content[start:]

        return FakeResponse()


_CONTENT = b"0123456789"


def _future(seconds: int = 3600) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _session_payload(
    url: str = "https://example.com/new", checksum: str = "abc"
) -> dict[str, Any]:
    return {
        "downloadUrl": url,
        "sizeBytes": str(len(_CONTENT)),
        "checksum": checksum,
        "expiresAt": _future(),
    }


def _write_interrupted_download(
    tmp_path: Path,
    url: str = "https://example.com/stored",
    checksum: str = "abc",
    expires_at: str | None = None,
    partial: bytes = _CONTENT[:4],
) -> Path:
    """Leave behind what an interrupted download of `dataset.tar.gz` leaves: a .part file
    and a sidecar with the stored plan. Returns the target path."""
    target = tmp_path / "dataset.tar.gz"
    target.with_name(target.name + ".part").write_bytes(partial)
    _get_checksum_filepath(target).write_text(
        json.dumps(
            {
                "checksum": checksum,
                "download_url": url,
                "expires_at": expires_at or _future(),
                "size_bytes": len(_CONTENT),
            }
        )
    )
    return target


def _download(tmp_path: Path, **kwargs: Any) -> Path:
    return _download_dataset(
        dataset_id="dataset-id",
        archive_filename="dataset.tar.gz",
        download_directory=str(tmp_path),
        show_progress=False,
        overwrite_existing=kwargs.pop("overwrite_existing", False),
        **kwargs,
    )


def test_run_download_plan_writes_url_and_expiry_to_sidecar(
    tmp_path: Path, monkeypatch
) -> None:
    api = _FakeDatasetApi(_session_payload(), _CONTENT)
    monkeypatch.setattr("datacollective.download._send_api_request", api)
    plan = DownloadPlan.model_validate(
        {
            **_make_download_plan(tmp_path, checksum="abc").model_dump(),
            "size_bytes": len(_CONTENT),
            "expires_at": _future(),
        }
    )
    written: dict[str, Any] = {}

    def capture_then_fail(**kwargs: Any) -> None:
        written.update(json.loads(plan.checksum_filepath.read_text()))
        raise RuntimeError("stop here")

    monkeypatch.setattr(
        "datacollective.download._execute_download_plan", capture_then_fail
    )

    with pytest.raises(RuntimeError, match="stop here"):
        _run_download_plan(plan, show_progress=False, overwrite_existing=False)

    assert written == {
        "checksum": "abc",
        "download_url": plan.download_url,
        "expires_at": plan.expires_at.isoformat().replace("+00:00", "Z"),  # type: ignore[union-attr]
        "size_bytes": len(_CONTENT),
    }


def test_resume_uses_stored_url_without_calling_the_api(
    tmp_path: Path, monkeypatch
) -> None:
    target = _write_interrupted_download(tmp_path)
    api = _FakeDatasetApi(_session_payload(), _CONTENT)
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    result = _download(tmp_path)

    assert result == target
    assert target.read_bytes() == _CONTENT
    assert api.methods == ["GET"]
    assert api.calls[0]["url"] == "https://example.com/stored"
    assert api.calls[0]["extra_headers"]["Range"] == "bytes=4-"
    assert not _get_checksum_filepath(target).exists()
    assert not target.with_name(target.name + ".part").exists()


def test_resume_asks_the_api_when_stored_url_has_expired(
    tmp_path: Path, monkeypatch
) -> None:
    target = _write_interrupted_download(tmp_path, expires_at=_future(seconds=10))
    api = _FakeDatasetApi(_session_payload(), _CONTENT)
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    _download(tmp_path)

    assert target.read_bytes() == _CONTENT
    assert api.methods == ["POST", "GET"]
    # The fresh URL is used, and the partial download is still resumed.
    assert api.calls[1]["url"] == "https://example.com/new"
    assert api.calls[1]["extra_headers"]["Range"] == "bytes=4-"


def test_resume_falls_back_to_the_api_when_stored_url_is_rejected(
    tmp_path: Path, monkeypatch
) -> None:
    target = _write_interrupted_download(tmp_path)
    api = _FakeDatasetApi(
        _session_payload(), _CONTENT, rejected_urls=("https://example.com/stored",)
    )
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    _download(tmp_path)

    assert target.read_bytes() == _CONTENT
    assert api.methods == ["GET", "POST", "GET"]
    assert api.calls[0]["url"] == "https://example.com/stored"
    assert api.calls[2]["url"] == "https://example.com/new"
    assert api.calls[2]["extra_headers"]["Range"] == "bytes=4-"


def test_resume_with_stored_url_surfaces_other_download_failures(
    tmp_path: Path, monkeypatch
) -> None:
    _write_interrupted_download(tmp_path)

    def connection_lost(**kwargs: Any) -> Any:
        raise requests.ConnectionError("reset")

    monkeypatch.setattr("datacollective.download._send_api_request", connection_lost)
    monkeypatch.setattr("datacollective.download.time.sleep", lambda _s: None)

    with pytest.raises(DownloadError):
        _download(tmp_path, max_download_attempts=1)


def test_overwrite_ignores_stored_url(tmp_path: Path, monkeypatch) -> None:
    target = _write_interrupted_download(tmp_path)
    api = _FakeDatasetApi(_session_payload(), _CONTENT)
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    _download(tmp_path, overwrite_existing=True)

    assert target.read_bytes() == _CONTENT
    assert api.methods == ["POST", "GET"]
    assert "Range" not in (api.calls[1]["extra_headers"] or {})


def test_legacy_plain_text_sidecar_still_resumes_via_the_api(
    tmp_path: Path, monkeypatch
) -> None:
    target = tmp_path / "dataset.tar.gz"
    target.with_name(target.name + ".part").write_bytes(_CONTENT[:4])
    _get_checksum_filepath(target).write_text("abc")
    api = _FakeDatasetApi(_session_payload(), _CONTENT)
    monkeypatch.setattr("datacollective.download._send_api_request", api)

    _download(tmp_path)

    assert target.read_bytes() == _CONTENT
    assert api.methods == ["POST", "GET"]
    assert api.calls[1]["extra_headers"]["Range"] == "bytes=4-"


@pytest.mark.parametrize(
    "sidecar",
    [
        None,
        "",
        "not json {",
        json.dumps(["a", "list"]),
        json.dumps({"checksum": "abc"}),
        json.dumps({"checksum": "abc", "download_url": "https://x", "size_bytes": 10}),
        json.dumps(
            {
                "checksum": "abc",
                "download_url": "https://x",
                "size_bytes": 10,
                "extra": 1,
            }
        ),
    ],
)
def test_load_stored_download_plan_rejects_incomplete_state(
    tmp_path: Path, sidecar: str | None
) -> None:
    target = tmp_path / "dataset.tar.gz"
    target.with_name(target.name + ".part").write_bytes(b"0123")
    if sidecar is not None:
        _get_checksum_filepath(target).write_text(sidecar)

    assert _load_stored_download_plan(target) is None


def test_load_stored_download_plan_requires_part_file(tmp_path: Path) -> None:
    target = _write_interrupted_download(tmp_path)
    target.with_name(target.name + ".part").unlink()

    assert _load_stored_download_plan(target) is None


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
    assert "Starting fresh download" in caplog.text


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
