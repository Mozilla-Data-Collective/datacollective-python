from __future__ import annotations

import tarfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import yaml

from datacollective.datasets import _extract_archive, load_dataset
from datacollective.download import DownloadPlan
from datacollective.schema import ColumnMapping, DatasetSchema, parse_schema
from datacollective.schema_loaders.cache_schema import (
    _save_schema_to_disk,
    _load_cached_schema,
)
from datacollective.schema_loaders.registry import load_dataset_from_schema


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_tar_gz(archive_path: Path, root_dir: Path) -> None:
    """Create a .tar.gz from all files under *root_dir*."""
    with tarfile.open(archive_path, "w:gz") as tf:
        for f in root_dir.rglob("*"):
            if f.is_file():
                tf.add(f, arcname=f.relative_to(root_dir))


def _fake_download_plan(
    target_filepath: Path = Path("/tmp/ds.tar.gz"),
    checksum: str | None = "fake_checksum",
) -> DownloadPlan:
    """Create a DownloadPlan for testing."""
    return DownloadPlan(
        download_url="https://example.com/dl",
        filename=target_filepath.name,
        target_filepath=target_filepath,
        tmp_filepath=target_filepath.with_name(target_filepath.name + ".part"),
        size_bytes=1000,
        checksum=checksum,
        checksum_filepath=target_filepath.with_name(target_filepath.name + ".checksum"),
    )


class TestExtractArchive:
    def test_extract_tar_gz(self, tmp_path: Path) -> None:
        # Prepare a tiny tar.gz
        src = tmp_path / "src"
        _write(src / "hello.txt", "world")
        archive = tmp_path / "archive.tar.gz"
        _make_tar_gz(archive, src)

        result = _extract_archive(archive, dest_dir=tmp_path, overwrite_extracted=False)
        assert result.exists()
        assert (result / "hello.txt").read_text() == "world"

    def test_skip_if_already_extracted(self, tmp_path: Path) -> None:
        src = tmp_path / "src"
        _write(src / "hello.txt", "world")
        archive = tmp_path / "archive.tar.gz"
        _make_tar_gz(archive, src)

        # First extraction
        result1 = _extract_archive(
            archive, dest_dir=tmp_path, overwrite_extracted=False
        )
        # Write a sentinel to prove re-extraction didn't happen
        sentinel = result1 / "sentinel.txt"
        sentinel.write_text("marker")

        result2 = _extract_archive(
            archive, dest_dir=tmp_path, overwrite_extracted=False
        )
        assert result2 == result1
        assert sentinel.exists()  # was NOT deleted

    def test_overwrite_extracted(self, tmp_path: Path) -> None:
        src = tmp_path / "src"
        _write(src / "hello.txt", "world")
        archive = tmp_path / "archive.tar.gz"
        _make_tar_gz(archive, src)

        result1 = _extract_archive(
            archive, dest_dir=tmp_path, overwrite_extracted=False
        )
        sentinel = result1 / "sentinel.txt"
        sentinel.write_text("marker")

        result2 = _extract_archive(archive, dest_dir=tmp_path, overwrite_extracted=True)
        assert result2 == result1
        assert not sentinel.exists()  # directory was re-created

    def test_unsupported_archive_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "archive.rar"
        bad.write_bytes(b"\x00")
        with pytest.raises(ValueError, match="Unsupported archive type"):
            _extract_archive(bad, dest_dir=tmp_path, overwrite_extracted=False)


class TestLoadDataset:
    """Tests for the full load_dataset pipeline with mocked network calls."""

    def _setup_asr_dataset(self, tmp_path: Path) -> tuple[Path, DatasetSchema]:
        """Create a synthetic ASR dataset archive and return (archive_path, schema)."""
        src = tmp_path / "src"
        _write(
            src / "train.tsv", "path\tsentence\nclip1.mp3\thello\nclip2.mp3\tworld\n"
        )

        archive = tmp_path / "downloads" / "ds-abc123.tar.gz"
        archive.parent.mkdir(parents=True)
        _make_tar_gz(archive, src)

        schema = DatasetSchema(
            dataset_id="test-ds",
            task="ASR",
            format="tsv",
            index_file="train.tsv",
            columns={
                "audio_path": ColumnMapping(source_column="path", dtype="file_path"),
                "transcription": ColumnMapping(
                    source_column="sentence", dtype="string"
                ),
            },
        )
        return archive, schema

    @patch("datacollective.datasets._resolve_schema")
    @patch("datacollective.datasets.save_dataset_to_disk")
    @patch("datacollective.datasets.get_download_plan")
    @patch("datacollective.datasets.get_dataset_schema")
    def test_load_dataset_asr_index(
        self,
        mock_get_schema,
        mock_get_plan,
        mock_save,
        mock_resolve,
        tmp_path: Path,
    ) -> None:
        archive, schema = self._setup_asr_dataset(tmp_path)

        # Mock the full pipeline
        mock_get_schema.return_value = schema
        plan = _fake_download_plan(
            target_filepath=archive,
            checksum="ck1",
        )
        mock_get_plan.return_value = plan
        mock_save.return_value = archive

        # Pre-extract so _extract_archive finds the directory
        _extract_archive(
            archive, dest_dir=tmp_path / "downloads", overwrite_extracted=False
        )

        mock_resolve.return_value = schema

        df = load_dataset(
            "test-ds",
            download_directory=str(tmp_path / "downloads"),
            show_progress=False,
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "audio_path" in df.columns
        assert "transcription" in df.columns
        mock_get_schema.assert_called_once_with("test-ds")

    @patch("datacollective.datasets.get_dataset_schema")
    def test_load_dataset_no_schema_raises(self, mock_get_schema) -> None:
        mock_get_schema.return_value = None
        with pytest.raises(RuntimeError, match="not supported"):
            load_dataset("unknown-ds")

    def _setup_tts_paired_dataset(self, tmp_path: Path) -> tuple[Path, DatasetSchema]:
        """Create a synthetic TTS paired-glob dataset archive."""
        src = tmp_path / "src"
        for name in ("001", "002"):
            d = src / "split_a"
            d.mkdir(parents=True, exist_ok=True)
            _write(d / f"{name}.txt", f"text {name}")
            (d / f"{name}.webm").write_bytes(b"\x00")

        archive = tmp_path / "downloads" / "tts-ds.tar.gz"
        archive.parent.mkdir(parents=True)
        _make_tar_gz(archive, src)

        schema = DatasetSchema(
            dataset_id="tts-ds",
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
        )
        return archive, schema

    @patch("datacollective.datasets._resolve_schema")
    @patch("datacollective.datasets.save_dataset_to_disk")
    @patch("datacollective.datasets.get_download_plan")
    @patch("datacollective.datasets.get_dataset_schema")
    def test_load_dataset_tts_paired_glob(
        self,
        mock_get_schema,
        mock_get_plan,
        mock_save,
        mock_resolve,
        tmp_path: Path,
    ) -> None:
        archive, schema = self._setup_tts_paired_dataset(tmp_path)

        mock_get_schema.return_value = schema
        plan = _fake_download_plan(target_filepath=archive, checksum="ck2")
        mock_get_plan.return_value = plan
        mock_save.return_value = archive

        _extract_archive(
            archive, dest_dir=tmp_path / "downloads", overwrite_extracted=False
        )
        mock_resolve.return_value = schema

        df = load_dataset(
            "tts-ds",
            download_directory=str(tmp_path / "downloads"),
            show_progress=False,
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "audio_path" in df.columns
        assert "transcription" in df.columns
        assert "split" in df.columns


class TestSchemaLoadingE2E:
    """
    Full integration tests that exercise parse_schema -> registry -> loader
    on synthetic data on disk, without any mocking.  These verify the complete
    schema-to-DataFrame pipeline.
    """

    def test_asr_index_full_pipeline(self, tmp_path: Path) -> None:
        """ASR index-based: schema.yaml + TSV -> DataFrame."""
        _write(
            tmp_path / "train.tsv",
            "path\tsentence\nclip1.mp3\thello\nclip2.mp3\tworld\n",
        )
        schema_data = {
            "dataset_id": "test-asr",
            "task": "ASR",
            "format": "tsv",
            "index_file": "train.tsv",
            "base_audio_path": "clips/",
            "columns": {
                "audio_path": {"source_column": "path", "dtype": "file_path"},
                "transcription": {"source_column": "sentence", "dtype": "string"},
            },
        }
        _write(tmp_path / "schema.yaml", yaml.dump(schema_data))

        schema = parse_schema(tmp_path / "schema.yaml")
        df = load_dataset_from_schema(schema, tmp_path)

        assert len(df) == 2
        assert df["transcription"].tolist() == ["hello", "world"]
        assert all("clips" in p for p in df["audio_path"])

    def test_asr_multi_split_full_pipeline(self, tmp_path: Path) -> None:
        """ASR multi-split: multiple TSV files -> concatenated DataFrame with split column."""
        _write(tmp_path / "train.tsv", "path\tsentence\nt1.mp3\ttrain_text\n")
        _write(tmp_path / "dev.tsv", "path\tsentence\nd1.mp3\tdev_text\n")
        _write(tmp_path / "test.tsv", "path\tsentence\ntest1.mp3\ttest_text\n")

        schema_data = {
            "dataset_id": "test-asr-ms",
            "task": "ASR",
            "root_strategy": "multi_split",
            "splits": ["train", "dev", "test"],
            "columns": {
                "audio": {"source_column": "path", "dtype": "file_path"},
                "text": {"source_column": "sentence", "dtype": "string"},
            },
        }
        _write(tmp_path / "schema.yaml", yaml.dump(schema_data))

        schema = parse_schema(tmp_path / "schema.yaml")
        df = load_dataset_from_schema(schema, tmp_path)

        assert len(df) == 3
        assert set(df["split"]) == {"train", "dev", "test"}

    def test_tts_index_headerless_full_pipeline(self, tmp_path: Path) -> None:
        """TTS index-based with headerless pipe-delimited file."""
        _write(tmp_path / "metadata.csv", "clip1.mp3|hello world\nclip2.mp3|goodbye\n")

        schema_data = {
            "dataset_id": "test-tts-idx",
            "task": "TTS",
            "format": "pipe",
            "separator": "|",
            "has_header": False,
            "index_file": "metadata.csv",
            "base_audio_path": "wavs/",
            "columns": {
                "audio_path": {"source_column": 0, "dtype": "file_path"},
                "transcription": {"source_column": 1, "dtype": "string"},
            },
        }
        _write(tmp_path / "schema.yaml", yaml.dump(schema_data))

        schema = parse_schema(tmp_path / "schema.yaml")
        df = load_dataset_from_schema(schema, tmp_path)

        assert len(df) == 2
        assert df["transcription"].tolist() == ["hello world", "goodbye"]
        assert all("wavs" in p for p in df["audio_path"])

    def test_tts_paired_glob_full_pipeline(self, tmp_path: Path) -> None:
        """TTS paired-glob: txt + audio files -> DataFrame."""
        for split in ("domain_a", "domain_b"):
            d = tmp_path / split
            d.mkdir()
            for i in range(3):
                _write(d / f"{i:04d}.txt", f"Text {split} {i}")
                (d / f"{i:04d}.webm").write_bytes(b"\x00")

        schema_data = {
            "dataset_id": "test-tts-pg",
            "task": "TTS",
            "root_strategy": "paired_glob",
            "file_pattern": "**/*.txt",
            "audio_extension": ".webm",
        }
        _write(tmp_path / "schema.yaml", yaml.dump(schema_data))

        schema = parse_schema(tmp_path / "schema.yaml")
        df = load_dataset_from_schema(schema, tmp_path)

        assert len(df) == 6
        assert set(df["split"]) == {"domain_a", "domain_b"}
        assert all(p.endswith(".webm") for p in df["audio_path"])

    def test_schema_cache_round_trip(self, tmp_path: Path) -> None:
        """Save -> load round-trip via cache_schema preserves all fields."""

        original = DatasetSchema(
            dataset_id="ds1",
            task="TTS",
            format="pipe",
            separator="|",
            has_header=False,
            index_file="meta.csv",
            base_audio_path="wavs/",
            encoding="utf-8-sig",
            checksum="abc123",
            columns={
                "audio": ColumnMapping(source_column=0, dtype="file_path"),
                "text": ColumnMapping(source_column=1, dtype="string", optional=True),
            },
        )
        schema_path = tmp_path / "schema.yaml"
        _save_schema_to_disk(original, schema_path)

        restored = _load_cached_schema(schema_path)
        assert restored is not None
        assert restored.dataset_id == original.dataset_id
        assert restored.task == original.task
        assert restored.format == original.format
        assert restored.separator == original.separator
        assert restored.has_header == original.has_header
        assert restored.encoding == original.encoding
        assert restored.checksum == original.checksum
        assert restored.base_audio_path == original.base_audio_path
        assert "audio" in restored.columns
        assert restored.columns["audio"].source_column == 0
        assert restored.columns["audio"].dtype == "file_path"
        assert restored.columns["text"].optional is True

    def test_schema_cache_round_trip_paired_glob(self, tmp_path: Path) -> None:
        """Round-trip for a paired-glob schema."""

        original = DatasetSchema(
            dataset_id="ds2",
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
            checksum="xyz",
        )
        schema_path = tmp_path / "schema.yaml"
        _save_schema_to_disk(original, schema_path)

        restored = _load_cached_schema(schema_path)
        assert restored is not None
        assert restored.root_strategy == "paired_glob"
        assert restored.file_pattern == "**/*.txt"
        assert restored.audio_extension == ".webm"

    def test_schema_cache_round_trip_multi_split(self, tmp_path: Path) -> None:
        """Round-trip for a multi-split schema."""

        original = DatasetSchema(
            dataset_id="ds3",
            task="ASR",
            root_strategy="multi_split",
            splits=["train", "dev", "test"],
            splits_file_pattern="**/*.tsv",
            checksum="ck",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        schema_path = tmp_path / "schema.yaml"
        _save_schema_to_disk(original, schema_path)

        restored = _load_cached_schema(schema_path)
        assert restored is not None
        assert restored.root_strategy == "multi_split"
        assert restored.splits == ["train", "dev", "test"]
        assert restored.splits_file_pattern == "**/*.tsv"
        assert "audio" in restored.columns
