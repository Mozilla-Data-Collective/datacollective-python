from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from datacollective.schema import (
    ColumnMapping,
    DatasetSchema,
    _parse_schema,
)


class TestDatasetSchema:
    def test_minimal(self) -> None:
        s = DatasetSchema(dataset_id="ds1", task="ASR")
        assert s.dataset_id == "ds1"
        assert s.task == "ASR"
        assert s.format is None
        assert s.columns == {}
        assert s.has_header is True
        assert s.encoding == "utf-8"
        assert s.extra == {}

    def test_mutable(self) -> None:
        """DatasetSchema must be mutable (frozen=False) for cache stamping."""
        s = DatasetSchema(dataset_id="ds1", task="ASR")
        s.checksum = "abc123"
        assert s.checksum == "abc123"

    def test_to_yaml_dict_minimal(self) -> None:
        s = DatasetSchema(dataset_id="ds1", task="ASR")
        d = s.to_yaml_dict()
        assert d == {"dataset_id": "ds1", "task": "ASR"}

    def test_to_yaml_dict_excludes_defaults(self) -> None:
        s = DatasetSchema(
            dataset_id="ds1", task="ASR", has_header=True, encoding="utf-8"
        )
        d = s.to_yaml_dict()
        # has_header and encoding are at their defaults -> excluded
        assert "has_header" not in d
        assert "encoding" not in d

    def test_to_yaml_dict_includes_non_defaults(self) -> None:
        s = DatasetSchema(
            dataset_id="ds1",
            task="TTS",
            format="tsv",
            index_file="meta.tsv",
            has_header=False,
            encoding="utf-8-sig",
            checksum="ck1",
        )
        d = s.to_yaml_dict()
        assert d["format"] == "tsv"
        assert d["index_file"] == "meta.tsv"
        assert d["has_header"] is False
        assert d["encoding"] == "utf-8-sig"
        assert d["checksum"] == "ck1"

    def test_to_yaml_dict_merges_extra(self) -> None:
        s = DatasetSchema(dataset_id="ds1", task="ASR", extra={"custom_key": 42})
        d = s.to_yaml_dict()
        assert d["custom_key"] == 42
        assert "extra" not in d

    def test_to_yaml_dict_serialises_columns(self) -> None:
        s = DatasetSchema(
            dataset_id="ds1",
            task="ASR",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(
                    source_column="sentence", dtype="string", optional=True
                ),
            },
        )
        d = s.to_yaml_dict()
        assert "columns" in d
        assert d["columns"]["audio"]["source_column"] == "path"
        assert d["columns"]["audio"]["dtype"] == "file_path"
        assert d["columns"]["text"]["optional"] is True

    def test_to_yaml_dict_round_trip(self) -> None:
        """to_yaml_dict -> yaml.dump -> parse_schema should round-trip."""
        original = DatasetSchema(
            dataset_id="ds1",
            task="TTS",
            format="tsv",
            index_file="meta.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
            },
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
            checksum="abc",
        )
        yaml_str = yaml.dump(original.to_yaml_dict())
        restored = _parse_schema(yaml_str)
        assert restored.dataset_id == original.dataset_id
        assert restored.task == original.task
        assert restored.format == original.format
        assert restored.root_strategy == original.root_strategy
        assert restored.checksum == original.checksum
        assert "audio" in restored.columns
        assert restored.columns["audio"].dtype == "file_path"


class TestParseSchema:
    def test_from_yaml_string(self) -> None:
        raw = "dataset_id: ds1\ntask: asr\n"
        s = _parse_schema(raw)
        assert s.dataset_id == "ds1"
        assert s.task == "ASR"  # upper-cased

    def test_from_dict(self) -> None:
        s = _parse_schema({"dataset_id": "ds1", "task": "tts"})
        assert s.task == "TTS"

    def test_from_file(self, tmp_path: Path) -> None:
        p = tmp_path / "schema.yaml"
        p.write_text(yaml.dump({"dataset_id": "ds1", "task": "ASR"}))
        s = _parse_schema(p)
        assert s.dataset_id == "ds1"

    def test_missing_dataset_id_raises(self) -> None:
        with pytest.raises(ValueError, match="dataset_id"):
            _parse_schema({"task": "ASR"})

    def test_missing_task_raises(self) -> None:
        with pytest.raises(ValueError, match="dataset_id.*task"):
            _parse_schema({"dataset_id": "ds1"})

    def test_invalid_yaml_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected a dict"):
            _parse_schema("just a string value")

    def test_columns_parsed(self) -> None:
        raw: dict[str, Any] = {
            "dataset_id": "ds1",
            "task": "ASR",
            "columns": {
                "audio_path": {"source_column": "path", "dtype": "file_path"},
                "text": {"source_column": "sentence"},
            },
        }
        s = _parse_schema(raw)
        assert "audio_path" in s.columns
        assert s.columns["audio_path"].dtype == "file_path"
        assert s.columns["text"].dtype == "string"  # default

    def test_columns_with_int_source(self) -> None:
        raw: dict[str, Any] = {
            "dataset_id": "ds1",
            "task": "TTS",
            "has_header": False,
            "columns": {
                "audio": {"source_column": 0, "dtype": "file_path"},
                "text": {"source_column": 1},
            },
        }
        s = _parse_schema(raw)
        assert s.columns["audio"].source_column == 0
        assert s.has_header is False

    def test_content_mapping_parsed(self) -> None:
        raw: dict[str, Any] = {
            "dataset_id": "ds1",
            "task": "LM",
            "content_mapping": {"text": "file_content", "meta_source": "file_name"},
        }
        s = _parse_schema(raw)
        assert s.content_mapping is not None
        assert s.content_mapping.text == "file_content"

    def test_unknown_keys_captured_in_extra(self) -> None:
        raw: dict[str, Any] = {
            "dataset_id": "ds1",
            "task": "ASR",
            "my_custom_field": "hello",
            "another": 42,
        }
        s = _parse_schema(raw)
        assert s.extra == {"my_custom_field": "hello", "another": 42}

    def test_root_strategy_parsed(self) -> None:
        raw: dict[str, Any] = {
            "dataset_id": "ds1",
            "task": "ASR",
            "root_strategy": "multi_split",
            "splits": ["train", "dev"],
        }
        s = _parse_schema(raw)
        assert s.root_strategy == "multi_split"

    def test_all_fields(self) -> None:
        """Comprehensive parsing with every known field populated."""
        raw: dict[str, Any] = {
            "dataset_id": "ds1",
            "task": "TTS",
            "format": "pipe",
            "index_file": "meta.csv",
            "base_audio_path": "wavs/",
            "separator": "|",
            "has_header": False,
            "encoding": "utf-8-sig",
            "root_strategy": "paired_glob",
            "file_pattern": "**/*.txt",
            "audio_extension": ".webm",
            "content_mapping": {"text": "fc"},
            "splits": ["train"],
            "splits_file_pattern": "**/*.csv",
            "checksum": "ck",
            "columns": {
                "a": {"source_column": 0, "dtype": "file_path", "optional": True},
            },
        }
        s = _parse_schema(raw)
        assert s.format == "pipe"
        assert s.separator == "|"
        assert s.has_header is False
        assert s.encoding == "utf-8-sig"
        assert s.splits == ["train"]
        assert s.splits_file_pattern == "**/*.csv"
        assert s.columns["a"].optional is True

    def test_non_dict_column_entries_ignored(self) -> None:
        raw: dict[str, Any] = {
            "dataset_id": "ds1",
            "task": "ASR",
            "columns": {"good": {"source_column": "x"}, "bad": "not_a_dict"},
        }
        s = _parse_schema(raw)
        assert "good" in s.columns
        assert "bad" not in s.columns
