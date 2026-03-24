from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.cache_schema import _resolve_schema


def _make_schema(
    dataset_id: str = "ds1", task: str = "TTS", checksum: str | None = None
) -> DatasetSchema:
    return DatasetSchema(dataset_id=dataset_id, task=task, checksum=checksum)


def _write_schema_yaml(
    path: Path, dataset_id: str = "ds1", task: str = "TTS", checksum: str | None = None
) -> None:
    data: dict = {"dataset_id": dataset_id, "task": task}
    if checksum is not None:
        data["checksum"] = checksum
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data), encoding="utf-8")


class TestCacheHit:
    """When cached schema checksum matches the archive checksum, no remote fetch should happen."""

    @patch("datacollective.schema_loaders.cache_schema._get_dataset_schema")
    def test_cache_hit_skips_remote_fetch(self, mock_get, tmp_path: Path) -> None:
        _write_schema_yaml(tmp_path / "schema.yaml", checksum="abc123")

        result = _resolve_schema("ds1", tmp_path, archive_checksum="abc123")

        mock_get.assert_not_called()
        assert result.dataset_id == "ds1"
        assert result.checksum == "abc123"

    @patch("datacollective.schema_loaders.cache_schema._get_dataset_schema")
    def test_cache_hit_returns_all_fields(self, mock_get, tmp_path: Path) -> None:
        schema_path = tmp_path / "schema.yaml"
        data = {
            "dataset_id": "ds1",
            "task": "TTS",
            "checksum": "abc123",
            "root_strategy": "paired_glob",
            "file_pattern": "**/*.txt",
            "audio_extension": ".webm",
        }
        schema_path.write_text(yaml.dump(data), encoding="utf-8")

        result = _resolve_schema("ds1", tmp_path, archive_checksum="abc123")

        mock_get.assert_not_called()
        assert result.root_strategy == "paired_glob"
        assert result.file_pattern == "**/*.txt"
        assert result.audio_extension == ".webm"


class TestCacheMiss:
    """When cache doesn't match (or doesn't exist), a remote fetch should happen."""

    @patch("datacollective.schema_loaders.cache_schema._get_dataset_schema")
    def test_no_cache_fetches_remote_and_stamps_checksum(
        self, mock_get, tmp_path: Path
    ) -> None:
        remote = _make_schema(checksum=None)
        mock_get.return_value = remote

        result = _resolve_schema("ds1", tmp_path, archive_checksum="new_checksum")

        mock_get.assert_called_once_with("ds1")
        assert result.checksum == "new_checksum"
        # Verify schema was saved to disk with the archive checksum
        saved = yaml.safe_load((tmp_path / "schema.yaml").read_text())
        assert saved["checksum"] == "new_checksum"

    @patch("datacollective.schema_loaders.cache_schema._get_dataset_schema")
    def test_checksum_mismatch_fetches_remote(self, mock_get, tmp_path: Path) -> None:
        _write_schema_yaml(tmp_path / "schema.yaml", checksum="old_checksum")
        remote = _make_schema(checksum=None)
        mock_get.return_value = remote

        result = _resolve_schema("ds1", tmp_path, archive_checksum="new_checksum")

        mock_get.assert_called_once_with("ds1")
        assert result.checksum == "new_checksum"

    @patch("datacollective.schema_loaders.cache_schema._get_dataset_schema")
    def test_cached_without_checksum_fetches_remote(
        self, mock_get, tmp_path: Path
    ) -> None:
        _write_schema_yaml(tmp_path / "schema.yaml", checksum=None)
        remote = _make_schema(checksum=None)
        mock_get.return_value = remote

        result = _resolve_schema("ds1", tmp_path, archive_checksum="archive_cksum")

        mock_get.assert_called_once_with("ds1")
        assert result.checksum == "archive_cksum"


class TestFallbacks:
    """Edge cases: remote not found, no cache, etc."""

    @patch("datacollective.schema_loaders.cache_schema._get_dataset_schema")
    def test_remote_not_found_falls_back_to_cache(
        self, mock_get, tmp_path: Path
    ) -> None:
        _write_schema_yaml(tmp_path / "schema.yaml", checksum="old")
        mock_get.return_value = None

        result = _resolve_schema("ds1", tmp_path, archive_checksum="different")

        assert result.dataset_id == "ds1"
        assert result.checksum == "old"

    @patch("datacollective.schema_loaders.cache_schema._get_dataset_schema")
    def test_remote_not_found_no_cache_raises(self, mock_get, tmp_path: Path) -> None:
        mock_get.return_value = None

        with pytest.raises(ValueError, match="not found in the schema registry"):
            _resolve_schema("ds1", tmp_path, archive_checksum="some_checksum")

    @patch("datacollective.schema_loaders.cache_schema._get_dataset_schema")
    def test_no_cache_no_archive_checksum_fetches_remote(
        self, mock_get, tmp_path: Path
    ) -> None:
        remote = _make_schema(checksum=None)
        mock_get.return_value = remote

        result = _resolve_schema("ds1", tmp_path, archive_checksum=None)

        mock_get.assert_called_once_with("ds1")
        # No archive checksum to stamp
        assert result.checksum is None

    @patch("datacollective.schema_loaders.cache_schema._get_dataset_schema")
    def test_default_archive_checksum_is_none(self, mock_get, tmp_path: Path) -> None:
        """Calling _resolve_schema without archive_checksum should work (backward compat)."""
        remote = _make_schema(checksum=None)
        mock_get.return_value = remote

        result = _resolve_schema("ds1", tmp_path)

        mock_get.assert_called_once_with("ds1")
        assert result.checksum is None
