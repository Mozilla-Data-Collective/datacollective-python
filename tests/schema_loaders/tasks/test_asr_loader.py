from __future__ import annotations

from pathlib import Path

import pytest

from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.tasks.asr import ASRLoader


def _write_tsv(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestASRLoaderValidation:
    def test_index_requires_index_file(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            columns={"a": ColumnMapping(source_column="x")},
        )
        with pytest.raises(ValueError, match="index_file"):
            ASRLoader(schema, tmp_path)

    def test_index_requires_columns(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds", task="ASR", format="tsv", index_file="f.tsv"
        )
        with pytest.raises(ValueError, match="column mapping"):
            ASRLoader(schema, tmp_path)

    def test_multi_split_requires_splits(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="ds", task="ASR", root_strategy="multi_split")
        with pytest.raises(ValueError, match="splits"):
            ASRLoader(schema, tmp_path)


class TestASRIndexBased:
    def test_load_tsv_without_format(self, tmp_path: Path) -> None:
        _write_tsv(
            tmp_path / "train.tsv",
            "path\tsentence\nclip1.mp3\thello\nclip2.mp3\tworld\n",
        )

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            index_file="train.tsv",
            columns={
                "audio_path": ColumnMapping(source_column="path", dtype="file_path"),
                "transcription": ColumnMapping(
                    source_column="sentence", dtype="string"
                ),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert list(df.columns) == ["audio_path", "transcription"]

    def test_load_tsv(self, tmp_path: Path) -> None:
        _write_tsv(
            tmp_path / "train.tsv",
            "path\tsentence\nclip1.mp3\thello\nclip2.mp3\tworld\n",
        )

        schema = DatasetSchema(
            dataset_id="ds",
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
        df = ASRLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert list(df.columns) == ["audio_path", "transcription"]
        assert df["transcription"].iloc[0] == "hello"

    def test_load_csv(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "data.csv", "path,sentence\nc1.mp3,hi\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="csv",
            index_file="data.csv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert len(df) == 1
        assert "audio" in df.columns

    def test_file_path_dtype_resolves_absolute(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "index.tsv", "path\nclip.mp3\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="index.tsv",
            base_audio_path="clips/",
            columns={"audio": ColumnMapping(source_column="path", dtype="file_path")},
        )
        df = ASRLoader(schema, tmp_path).load()
        expected = str(tmp_path / "clips" / "clip.mp3")
        assert df["audio"].iloc[0] == expected

    def test_file_path_dtype_resolves_absolute_from_relative_extract_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset_dir = tmp_path / "dataset"
        _write_tsv(dataset_dir / "index.tsv", "path\nclip.mp3\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="index.tsv",
            base_audio_path="clips/",
            columns={"audio": ColumnMapping(source_column="path", dtype="file_path")},
        )

        monkeypatch.chdir(tmp_path)
        df = ASRLoader(schema, Path("dataset")).load()

        assert Path(df["audio"].iloc[0]).is_absolute()
        assert df["audio"].iloc[0] == str(dataset_dir / "clips" / "clip.mp3")

    def test_file_path_uses_first_existing_base_audio_path(
        self, tmp_path: Path
    ) -> None:
        _write_tsv(tmp_path / "index.tsv", "path\nclip.wav\n")
        audio_path = tmp_path / "secondary" / "clip.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="index.tsv",
            base_audio_path=["primary/", "secondary/"],
            columns={"audio": ColumnMapping(source_column="path", dtype="file_path")},
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_exact_search_uses_extension_and_recurses(
        self, tmp_path: Path
    ) -> None:
        _write_tsv(tmp_path / "index.tsv", "clip_id\nclip_001\n")
        audio_path = tmp_path / "audio" / "nested" / "clip_001.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="index.tsv",
            base_audio_path="audio/",
            columns={
                "audio": ColumnMapping(
                    source_column="clip_id",
                    dtype="file_path",
                    path_match_strategy="exact",
                    file_extension=".wav",
                )
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_contains_search_matches_substring(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "index.tsv", "clip_fragment\nclip_001\n")
        audio_path = tmp_path / "audio" / "nested" / "speaker_clip_001_take2.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="index.tsv",
            base_audio_path="audio/",
            columns={
                "audio": ColumnMapping(
                    source_column="clip_fragment",
                    dtype="file_path",
                    path_match_strategy="contains",
                    file_extension=".wav",
                )
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_value_already_includes_base_path(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "index.tsv", "path\ndata/recipes/clip.wav\n")
        audio_path = tmp_path / "data" / "recipes" / "clip.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="index.tsv",
            base_audio_path="data/recipes/",
            columns={"audio": ColumnMapping(source_column="path", dtype="file_path")},
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_template_builds_name_from_multiple_columns(
        self, tmp_path: Path
    ) -> None:
        _write_tsv(
            tmp_path / "dataset" / "data" / "metadata.csv",
            "Speaker ID,Sentence ID,Sentences\n"
            "f-adt1-0001,recipes_01_0001_0001,hello\n",
        )
        audio_path = (
            tmp_path
            / "dataset"
            / "data"
            / "recipes"
            / "f-adt1-0001_khm_recipes_01_0001_0001.wav"
        )
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            index_file="data/metadata.csv",
            base_audio_path=["data/recipes/", "data/giving_gift/"],
            columns={
                "audio": ColumnMapping(
                    source_column="Sentence ID",
                    dtype="file_path",
                    file_extension=".wav",
                    path_template="${Speaker ID}_khm_${Sentence ID}.wav",
                ),
                "text": ColumnMapping(source_column="Sentences"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_template_renders_dynamic_audio_root_from_metadata(
        self, tmp_path: Path
    ) -> None:
        _write_tsv(
            tmp_path / "dataset" / "data" / "metadata.csv",
            "Split,Speaker ID,Sentence ID,Sentences\n"
            "recipes,f-adt1-0001,recipes_01_0001_0001,hello\n",
        )
        audio_path = (
            tmp_path
            / "dataset"
            / "data"
            / "recipes"
            / "f-adt1-0001_khm_recipes_01_0001_0001.wav"
        )
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            index_file="data/metadata.csv",
            base_audio_path="data/${Split}/",
            columns={
                "audio": ColumnMapping(
                    source_column="Sentence ID",
                    dtype="file_path",
                    file_extension=".wav",
                    path_template="${Speaker ID}_khm_${value}",
                ),
                "text": ColumnMapping(source_column="Sentences"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)
        assert df["text"].iloc[0] == "hello"

    def test_contains_search_raises_on_ambiguous_matches(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "index.tsv", "clip_fragment\nclip_001\n")
        audio_path_1 = tmp_path / "audio" / "nested" / "speaker_clip_001_take1.wav"
        audio_path_2 = tmp_path / "audio" / "nested" / "speaker_clip_001_take2.wav"
        audio_path_1.parent.mkdir(parents=True, exist_ok=True)
        audio_path_1.write_bytes(b"\x00")
        audio_path_2.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="index.tsv",
            base_audio_path="audio/",
            columns={
                "audio": ColumnMapping(
                    source_column="clip_fragment",
                    dtype="file_path",
                    path_match_strategy="contains",
                    file_extension=".wav",
                )
            },
        )
        with pytest.raises(ValueError, match="Ambiguous file_path value"):
            ASRLoader(schema, tmp_path).load()

    def test_category_dtype(self, tmp_path: Path) -> None:
        _write_tsv(
            tmp_path / "i.tsv", "path\tsentence\tspk\nc.mp3\thi\tA\nc2.mp3\tbye\tA\n"
        )

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="i.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
                "speaker": ColumnMapping(source_column="spk", dtype="category"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["speaker"].dtype.name == "category"

    def test_int_and_float_dtypes(self, tmp_path: Path) -> None:
        _write_tsv(
            tmp_path / "i.tsv", "path\tsentence\tdur\tscore\nc.mp3\thi\t100\t0.95\n"
        )

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="i.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
                "duration": ColumnMapping(source_column="dur", dtype="int"),
                "score": ColumnMapping(source_column="score", dtype="float"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["duration"].iloc[0] == 100
        assert df["score"].iloc[0] == pytest.approx(0.95)

    def test_optional_column_missing(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "i.tsv", "path\tsentence\nc.mp3\thi\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="i.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
                "speaker": ColumnMapping(
                    source_column="client_id", dtype="category", optional=True
                ),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert "speaker" not in df.columns  # silently skipped

    def test_required_column_missing_raises(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "i.tsv", "path\tsentence\nc.mp3\thi\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="i.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="nonexistent"),
            },
        )
        with pytest.raises(KeyError, match="nonexistent"):
            ASRLoader(schema, tmp_path).load()

    def test_index_file_not_found_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="missing.tsv",
            columns={"a": ColumnMapping(source_column="x")},
        )
        with pytest.raises(FileNotFoundError, match="missing.tsv"):
            ASRLoader(schema, tmp_path).load()

    def test_explicit_separator_overrides_format(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "d.csv", "path|sentence\nc.mp3|hi\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="csv",
            separator="|",
            index_file="d.csv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert len(df) == 1
        assert df["text"].iloc[0] == "hi"

    def test_sniffed_separator_and_trimmed_headers(self, tmp_path: Path) -> None:
        _write_tsv(
            tmp_path / "metadata.csv",
            "Topic; Sentence ID ; Sentences \nFood; clip.wav; hello\n",
        )
        (tmp_path / "clip.wav").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="csv",
            index_file="metadata.csv",
            columns={
                "audio": ColumnMapping(source_column="Sentence ID", dtype="file_path"),
                "text": ColumnMapping(source_column="Sentences"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(tmp_path / "clip.wav")
        assert df["text"].iloc[0] == "hello"

    def test_nested_index_file_found(self, tmp_path: Path) -> None:
        """Index file inside a subdirectory should be located via rglob."""
        nested = tmp_path / "sub" / "deep"
        _write_tsv(nested / "train.tsv", "path\tsentence\nc.mp3\thi\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="tsv",
            index_file="train.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert len(df) == 1

    def test_file_content_dtype_reads_text_file(self, tmp_path: Path) -> None:
        _write_tsv(
            tmp_path / "index.csv",
            "audio,transcript\nclip.wav,transcripts/clip.txt\n",
        )
        txt_path = tmp_path / "transcripts" / "clip.txt"
        txt_path.parent.mkdir()
        txt_path.write_text("hello world\n", encoding="utf-8")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="csv",
            index_file="index.csv",
            columns={
                "audio": ColumnMapping(source_column="audio", dtype="file_path"),
                "text": ColumnMapping(source_column="transcript", dtype="file_content"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["text"].iloc[0] == "hello world"

    def test_file_content_dtype_with_file_extension(self, tmp_path: Path) -> None:
        _write_tsv(
            tmp_path / "index.csv",
            "audio,transcript\nclip.wav,transcripts/clip\n",
        )
        txt_path = tmp_path / "transcripts" / "clip.txt"
        txt_path.parent.mkdir()
        txt_path.write_text("resolved with extension\n", encoding="utf-8")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            format="csv",
            index_file="index.csv",
            columns={
                "audio": ColumnMapping(source_column="audio", dtype="file_path"),
                "text": ColumnMapping(
                    source_column="transcript",
                    dtype="file_content",
                    file_extension=".txt",
                ),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert df["text"].iloc[0] == "resolved with extension"


class TestASRMultiSplit:
    def test_load_multiple_splits(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")
        _write_tsv(tmp_path / "dev.tsv", "path\tsentence\nc2.mp3\tworld\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            root_strategy="multi_split",
            splits=["train", "dev"],
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        df = ASRLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert set(df["split"]) == {"train", "dev"}
        assert "audio" in df.columns
        assert "text" in df.columns

    def test_multi_split_without_columns(self, tmp_path: Path) -> None:
        """When no column mappings, raw columns + split should be returned."""
        _write_tsv(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            root_strategy="multi_split",
            splits=["train"],
        )
        df = ASRLoader(schema, tmp_path).load()
        assert "split" in df.columns
        assert "path" in df.columns  # raw column name
        assert df["split"].iloc[0] == "train"

    def test_multi_split_custom_pattern(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.csv", "path,sentence\nc1.mp3,hello\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            root_strategy="multi_split",
            splits=["train"],
            splits_file_pattern="**/*.csv",
            format="csv",
        )
        df = ASRLoader(schema, tmp_path).load()
        assert len(df) == 1

    def test_multi_split_ignores_unlisted_splits(self, tmp_path: Path) -> None:
        _write_tsv(tmp_path / "train.tsv", "path\tsentence\nc1.mp3\thello\n")
        _write_tsv(tmp_path / "other.tsv", "path\tsentence\nc2.mp3\tbye\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            root_strategy="multi_split",
            splits=["train"],  # only train, not "other"
        )
        df = ASRLoader(schema, tmp_path).load()
        assert len(df) == 1
        assert df["split"].iloc[0] == "train"

    def test_multi_split_no_matching_files_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            task="ASR",
            root_strategy="multi_split",
            splits=["nonexistent"],
        )
        with pytest.raises(RuntimeError, match="No split files"):
            ASRLoader(schema, tmp_path).load()


def _write_json_sidecar(path: Path, filename: str, n_utts: int = 2) -> None:
    import json

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
        "task": "ASR",
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


class TestASRPairedGlobJSONValidation:
    def test_requires_json_format(self, tmp_path: Path) -> None:
        schema = _paired_glob_json_schema(format="tsv")
        with pytest.raises(ValueError, match="format: json"):
            ASRLoader(schema, tmp_path)

    def test_requires_file_pattern(self, tmp_path: Path) -> None:
        schema = _paired_glob_json_schema(file_pattern=None)
        with pytest.raises(ValueError, match="file_pattern"):
            ASRLoader(schema, tmp_path)

    def test_requires_columns(self, tmp_path: Path) -> None:
        schema = _paired_glob_json_schema(columns={})
        with pytest.raises(ValueError, match="column mapping"):
            ASRLoader(schema, tmp_path)


class TestASRPairedGlobJSON:
    def test_one_row_per_record_with_flattened_meta(self, tmp_path: Path) -> None:
        _write_json_sidecar(tmp_path / "rec1.merged.json", "rec1.wav", n_utts=3)
        _write_json_sidecar(tmp_path / "rec2.merged.json", "rec2.wav", n_utts=2)
        (tmp_path / "rec1.wav").touch()
        (tmp_path / "rec2.wav").touch()

        df = ASRLoader(_paired_glob_json_schema(), tmp_path).load()

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

        df = ASRLoader(_paired_glob_json_schema(), tmp_path).load()
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
        df = ASRLoader(schema, tmp_path).load()
        assert len(df) == 1

    def test_missing_record_path_key_raises(self, tmp_path: Path) -> None:
        import json

        (tmp_path / "rec1.merged.json").write_text(
            json.dumps({"audio": {"filename": "rec1.wav"}}), encoding="utf-8"
        )

        with pytest.raises(KeyError, match="record_path 'transcriptions'"):
            ASRLoader(_paired_glob_json_schema(), tmp_path).load()

    def test_no_matching_files_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="No files matching"):
            ASRLoader(_paired_glob_json_schema(), tmp_path).load()
