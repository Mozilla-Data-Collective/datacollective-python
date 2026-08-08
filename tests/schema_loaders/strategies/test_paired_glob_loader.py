from __future__ import annotations

import json
from pathlib import Path

import pytest

from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.strategies.paired_glob import PairedGlobLoader


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestPairedGlobValidation:
    def test_missing_file_pattern_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            audio_extension=".webm",
        )
        with pytest.raises(ValueError, match="file_pattern"):
            PairedGlobLoader(schema, tmp_path)

    def test_text_variant_missing_audio_extension_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
        )
        with pytest.raises(ValueError, match="audio_extension"):
            PairedGlobLoader(schema, tmp_path)

    def test_json_variant_missing_columns_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            format="json",
            file_pattern="**/*.json",
        )
        with pytest.raises(ValueError, match="column mapping"):
            PairedGlobLoader(schema, tmp_path)


class TestPairedGlobText:
    def _setup_paired(self, root: Path) -> None:
        """Create a paired-glob dataset structure under root."""
        for split in ("split_a", "split_b"):
            d = root / split
            d.mkdir(parents=True)
            _write(d / "001.txt", f"Hello from {split}")
            # Create matching audio files
            (d / "001.webm").write_bytes(b"\x00")

    def test_load_paired_glob(self, tmp_path: Path) -> None:
        self._setup_paired(tmp_path)

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
        )
        df = PairedGlobLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert "audio_path" in df.columns
        assert "transcription" in df.columns
        assert "split" in df.columns
        assert set(df["split"]) == {"split_a", "split_b"}

    def test_paired_glob_skips_missing_audio(self, tmp_path: Path) -> None:
        d = tmp_path / "split"
        d.mkdir()
        _write(d / "001.txt", "hello")
        # No matching .webm -> should be skipped
        _write(d / "002.txt", "world")
        (d / "002.webm").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
        )
        df = PairedGlobLoader(schema, tmp_path).load()
        assert len(df) == 1
        assert df["transcription"].iloc[0] == "world"

    def test_paired_glob_no_text_files_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
        )
        with pytest.raises(FileNotFoundError, match="No files matching"):
            PairedGlobLoader(schema, tmp_path).load()

    def test_paired_glob_no_matching_audio_raises(self, tmp_path: Path) -> None:
        """Text files exist but none have matching audio -> error."""
        d = tmp_path / "split"
        d.mkdir()
        _write(d / "001.txt", "hello")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
        )
        with pytest.raises(FileNotFoundError, match="No paired"):
            PairedGlobLoader(schema, tmp_path).load()

    def test_paired_glob_reads_transcription_stripped(self, tmp_path: Path) -> None:
        d = tmp_path / "s"
        d.mkdir()
        _write(d / "001.txt", "  hello world  \n")
        (d / "001.wav").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".wav",
        )
        df = PairedGlobLoader(schema, tmp_path).load()
        assert df["transcription"].iloc[0] == "hello world"

    def test_paired_glob_audio_path_is_absolute(self, tmp_path: Path) -> None:
        d = tmp_path / "s"
        d.mkdir()
        _write(d / "001.txt", "hi")
        (d / "001.wav").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".wav",
        )
        df = PairedGlobLoader(schema, tmp_path).load()
        assert Path(df["audio_path"].iloc[0]).is_absolute()

    def test_paired_glob_audio_path_is_absolute_from_relative_extract_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset_dir = tmp_path / "dataset"
        d = dataset_dir / "s"
        d.mkdir(parents=True)
        _write(d / "001.txt", "hi")
        (d / "001.wav").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".wav",
        )

        monkeypatch.chdir(tmp_path)
        df = PairedGlobLoader(schema, Path("dataset")).load()

        assert Path(df["audio_path"].iloc[0]).is_absolute()
        assert df["audio_path"].iloc[0] == str(dataset_dir / "s" / "001.wav")


def _write_json_sidecar(path: Path, filename: str, n_utts: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "audio": {"filename": filename, "duration_sec": 12.5, "sample_rate_hz": 44100},
        "metadata": {"gender": "male", "id": "abc123"},
        "transcriptions": [
            {
                "utt_id": f"{Path(filename).stem}_{i:04d}",
                "speaker": f"SPEAKER{i % 2 + 1}",
                "start_time": i * 2.0,
                "end_time": i * 2.0 + 1.5,
                "text": f"utterance {i}",
            }
            for i in range(1, n_utts + 1)
        ],
    }
    path.write_text(json.dumps(data), encoding="utf-8")


def _paired_glob_json_schema(**overrides) -> DatasetSchema:
    fields = {
        "dataset_id": "ds",
        "root_strategy": "paired_glob",
        "format": "json",
        "file_pattern": "**/*.merged.json",
        "record_path": "transcriptions",
        "columns": {
            "audio_path": ColumnMapping(
                source_column="audio.filename",
                dtype="file_path",
                path_match_strategy="exact",
            ),
            "transcription": ColumnMapping(source_column="text"),
            "speaker_id": ColumnMapping(
                source_column="speaker", dtype="category", optional=True
            ),
            "start_time": ColumnMapping(
                source_column="start_time", dtype="float", optional=True
            ),
            "gender": ColumnMapping(
                source_column="metadata.gender",
                dtype="category",
                optional=True,
            ),
        },
    }
    fields.update(overrides)
    return DatasetSchema(**fields)


class TestPairedGlobJSON:
    def test_one_row_per_record_with_flattened_meta(self, tmp_path: Path) -> None:
        _write_json_sidecar(tmp_path / "rec1.merged.json", "rec1.wav", n_utts=3)
        _write_json_sidecar(tmp_path / "rec2.merged.json", "rec2.wav", n_utts=2)
        (tmp_path / "rec1.wav").touch()
        (tmp_path / "rec2.wav").touch()

        df = PairedGlobLoader(_paired_glob_json_schema(), tmp_path).load()

        assert len(df) == 5
        assert list(df.columns) == [
            "audio_path",
            "transcription",
            "speaker_id",
            "start_time",
            "gender",
        ]
        # Per-recording fields repeat on every utterance row
        assert set(Path(p).name for p in df["audio_path"]) == {"rec1.wav", "rec2.wav"}
        assert (df["gender"] == "male").all()
        assert df["start_time"].dtype == "float64"
        assert df["transcription"].iloc[0] == "utterance 1"

    def test_audio_resolved_via_exact_search(self, tmp_path: Path) -> None:
        """Audio referenced by bare filename resolves even in nested layouts."""
        _write_json_sidecar(tmp_path / "inner" / "rec1.merged.json", "rec1.wav")
        (tmp_path / "inner" / "rec1.wav").touch()

        df = PairedGlobLoader(_paired_glob_json_schema(), tmp_path).load()
        assert Path(df["audio_path"].iloc[0]).exists()

    def test_without_record_path_one_row_per_file(self, tmp_path: Path) -> None:
        _write_json_sidecar(tmp_path / "rec1.merged.json", "rec1.wav")
        (tmp_path / "rec1.wav").touch()

        schema = _paired_glob_json_schema(
            record_path=None,
            columns={
                "audio_path": ColumnMapping(
                    source_column="audio.filename",
                    dtype="file_path",
                    path_match_strategy="exact",
                ),
                "gender": ColumnMapping(
                    source_column="metadata.gender", dtype="category"
                ),
            },
        )
        df = PairedGlobLoader(schema, tmp_path).load()
        assert len(df) == 1

    def test_missing_record_path_key_raises(self, tmp_path: Path) -> None:
        (tmp_path / "rec1.merged.json").write_text(
            json.dumps({"audio": {"filename": "rec1.wav"}}), encoding="utf-8"
        )

        with pytest.raises(KeyError, match="record_path 'transcriptions'"):
            PairedGlobLoader(_paired_glob_json_schema(), tmp_path).load()

    def test_no_matching_files_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="No files matching"):
            PairedGlobLoader(_paired_glob_json_schema(), tmp_path).load()
