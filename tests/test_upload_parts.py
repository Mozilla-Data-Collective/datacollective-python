"""Tests for the concurrent part upload loop."""

import hashlib
import random
import threading
import time
from pathlib import Path
from typing import Callable

import pytest

import datacollective.upload_utils as upload_utils_module
from datacollective.errors import RateLimitError, ResourceRemovedError
from datacollective.upload_utils import (
    PresignedPartUrl,
    UploadState,
    _load_upload_state,
    _upload_parts_and_compute_checksum,
)

PART_SIZE = 4


class FakeResponse:
    def __init__(self, part_number: int) -> None:
        self.headers = {"ETag": f'"etag-{part_number}"'}


class FakeStorage:
    """Records presign and PUT calls; `on_put` can block or raise per part."""

    def __init__(self) -> None:
        self.presigned: list[int] = []
        self.put: list[int] = []
        self.lock = threading.Lock()
        self.on_put: Callable[[int], object] = lambda part_number: None

    def presign(
        self,
        file_upload_id: str,
        part_number: int,
        submission_id: str,
        is_sample: bool = False,
    ) -> PresignedPartUrl:
        with self.lock:
            self.presigned.append(part_number)
        return PresignedPartUrl(
            partNumber=part_number, url=f"https://storage.test/{part_number}"
        )

    def upload(self, url: str, payload: bytes, **kwargs: object) -> FakeResponse:
        part_number = int(url.rsplit("/", 1)[1])
        with self.lock:
            self.put.append(part_number)
        self.on_put(part_number)
        return FakeResponse(part_number)


@pytest.fixture
def fake_storage(monkeypatch) -> FakeStorage:
    storage = FakeStorage()
    monkeypatch.setattr(upload_utils_module, "_get_presigned_part_url", storage.presign)
    monkeypatch.setattr(upload_utils_module, "_upload_part_with_retry", storage.upload)
    return storage


def _write_file(tmp_path: Path, parts: int) -> tuple[Path, bytes]:
    # Last part is deliberately short so the hash covers a partial chunk too.
    data = bytes(range(PART_SIZE * parts - 1))
    path = tmp_path / "dataset.tar.gz"
    path.write_bytes(data)
    return path, data


def _build_state(file_size: int) -> UploadState:
    return UploadState(
        submissionId="submission",
        fileUploadId="file-upload",
        uploadId="upload-id",
        fileSize=file_size,
        partSize=PART_SIZE,
        filename="dataset.tar.gz",
        mimeType="application/gzip",
    )


def _run(
    tmp_path: Path,
    parts: int,
    max_workers: int,
    parts_by_number: dict[int, str] | None = None,
) -> tuple[tuple[int, str], bytes, dict[int, str], UploadState, Path]:
    path, data = _write_file(tmp_path, parts)
    state = _build_state(len(data))
    state_file = tmp_path / "state.json"
    parts_by_number = dict(parts_by_number or {})
    result = _upload_parts_and_compute_checksum(
        path=path,
        state=state,
        parts_by_number=parts_by_number,
        expected_parts=parts,
        progress_bar=None,
        state_file=state_file,
        max_workers=max_workers,
    )
    return result, data, parts_by_number, state, state_file


def test_single_worker_uploads_parts_in_order(
    tmp_path: Path, fake_storage: FakeStorage
) -> None:
    (bytes_read, checksum), data, parts_by_number, state, state_file = _run(
        tmp_path, parts=3, max_workers=1
    )

    assert fake_storage.put == [1, 2, 3]
    assert bytes_read == len(data)
    assert checksum == hashlib.sha256(data).hexdigest()
    assert sorted(parts_by_number) == [1, 2, 3]
    saved = _load_upload_state(state_file)
    assert saved is not None
    assert [part.partNumber for part in saved.parts] == [1, 2, 3]
    assert saved.parts[0].etag == "etag-1"


def test_concurrent_upload_uploads_every_part_exactly_once(
    tmp_path: Path, fake_storage: FakeStorage
) -> None:
    # Random delays shuffle the completion order between workers.
    fake_storage.on_put = lambda part_number: time.sleep(random.random() * 0.01)

    (bytes_read, checksum), data, parts_by_number, state, _ = _run(
        tmp_path, parts=7, max_workers=3
    )

    assert sorted(fake_storage.put) == list(range(1, 8))
    assert sorted(fake_storage.presigned) == list(range(1, 8))
    assert bytes_read == len(data)
    assert checksum == hashlib.sha256(data).hexdigest()
    assert [part.partNumber for part in state.parts] == list(range(1, 8))
    assert parts_by_number[7] == "etag-7"


def test_resume_hashes_but_does_not_reupload_existing_parts(
    tmp_path: Path, fake_storage: FakeStorage
) -> None:
    (bytes_read, checksum), data, parts_by_number, _, _ = _run(
        tmp_path, parts=3, max_workers=2, parts_by_number={2: "old-etag-2"}
    )

    assert sorted(fake_storage.put) == [1, 3]
    assert 2 not in fake_storage.presigned
    assert bytes_read == len(data)
    assert checksum == hashlib.sha256(data).hexdigest()
    assert parts_by_number == {1: "etag-1", 2: "old-etag-2", 3: "etag-3"}


def test_in_flight_parts_are_bounded_by_max_workers(
    tmp_path: Path, fake_storage: FakeStorage
) -> None:
    release = threading.Event()
    fake_storage.on_put = lambda part_number: release.wait(timeout=5)
    outcome: dict[str, object] = {}

    def run() -> None:
        try:
            outcome["result"] = _run(tmp_path, parts=9, max_workers=3)
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while len(fake_storage.put) < 3 and time.monotonic() < deadline:
            time.sleep(0.005)
        # Give the loop a chance to (wrongly) submit more parts.
        time.sleep(0.05)
        assert len(fake_storage.put) == 3, "more parts in flight than max_workers"
    finally:
        release.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert "error" not in outcome, outcome.get("error")
    assert sorted(fake_storage.put) == list(range(1, 10))


@pytest.mark.parametrize("exc_type", [ResourceRemovedError, RuntimeError])
def test_first_failure_stops_submission_and_keeps_successful_parts(
    tmp_path: Path, fake_storage: FakeStorage, exc_type: type[Exception]
) -> None:
    def fail_on_part_3(part_number: int) -> None:
        if part_number == 3:
            raise exc_type("part 3 failed")

    fake_storage.on_put = fail_on_part_3

    with pytest.raises(exc_type, match="part 3 failed"):
        _run(tmp_path, parts=6, max_workers=2)

    # Part 4 may have been submitted alongside part 3, but nothing after it.
    assert max(fake_storage.put) <= 4
    saved = _load_upload_state(tmp_path / "state.json")
    assert saved is not None
    saved_numbers = {part.partNumber for part in saved.parts}
    assert {1, 2} <= saved_numbers
    assert 3 not in saved_numbers


def test_rate_limit_on_part_url_adds_tuning_hint(
    tmp_path: Path, fake_storage: FakeStorage, monkeypatch
) -> None:
    def rate_limited_presign(*_, **__):
        raise RateLimitError()

    monkeypatch.setattr(
        upload_utils_module, "_get_presigned_part_url", rate_limited_presign
    )

    with pytest.raises(RateLimitError, match="lowering `max_workers`") as exc_info:
        _run(tmp_path, parts=3, max_workers=2)

    assert isinstance(exc_info.value.__cause__, RateLimitError)
    assert fake_storage.put == []


def test_rate_limit_from_storage_adds_tuning_hint(
    tmp_path: Path, fake_storage: FakeStorage
) -> None:
    def throttled(part_number: int) -> None:
        raise RateLimitError()

    fake_storage.on_put = throttled

    with pytest.raises(RateLimitError, match="raising `part_size`"):
        _run(tmp_path, parts=3, max_workers=2)


def test_in_flight_success_after_failure_is_persisted(
    tmp_path: Path, fake_storage: FakeStorage
) -> None:
    def on_put(part_number: int) -> None:
        if part_number == 1:
            raise RuntimeError("part 1 failed")
        time.sleep(0.1)

    fake_storage.on_put = on_put

    with pytest.raises(RuntimeError, match="part 1 failed"):
        _run(tmp_path, parts=2, max_workers=2)

    saved = _load_upload_state(tmp_path / "state.json")
    assert saved is not None
    assert [part.partNumber for part in saved.parts] == [2]
