from __future__ import annotations

from pathlib import Path

import pytest

from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.strategies.index import IndexLoader


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestIndexLoaderValidation:
    def test_requires_index_file(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            columns={"a": ColumnMapping(source_column="x")},
        )
        with pytest.raises(ValueError, match="index_file"):
            IndexLoader(schema, tmp_path)


class TestIndexLoader:
    def test_load_tsv_without_format(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "train.tsv",
            "path\tsentence\nclip1.mp3\thello\nclip2.mp3\tworld\n",
        )

        schema = DatasetSchema(
            dataset_id="ds",
            index_file="train.tsv",
            columns={
                "audio_path": ColumnMapping(source_column="path", dtype="file_path"),
                "transcription": ColumnMapping(
                    source_column="sentence", dtype="string"
                ),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert list(df.columns) == ["audio_path", "transcription"]

    def test_load_tsv(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "train.tsv",
            "path\tsentence\nclip1.mp3\thello\nclip2.mp3\tworld\n",
        )

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="train.tsv",
            columns={
                "audio_path": ColumnMapping(source_column="path", dtype="file_path"),
                "transcription": ColumnMapping(
                    source_column="sentence", dtype="string"
                ),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert list(df.columns) == ["audio_path", "transcription"]
        assert df["transcription"].iloc[0] == "hello"

    def test_load_csv(self, tmp_path: Path) -> None:
        _write(tmp_path / "data.csv", "path,sentence\nc1.mp3,hi\n")

        schema = DatasetSchema(
            dataset_id="ds",
            format="csv",
            index_file="data.csv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert len(df) == 1
        assert "audio" in df.columns

    def test_load_pipe_delimited_headerless(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.csv", "clip1.mp3|hello world\nclip2.mp3|goodbye\n")

        schema = DatasetSchema(
            dataset_id="ds",
            format="pipe",
            separator="|",
            has_header=False,
            index_file="meta.csv",
            base_audio_path="wavs/",
            columns={
                "audio_path": ColumnMapping(source_column=0, dtype="file_path"),
                "transcription": ColumnMapping(source_column=1, dtype="string"),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert df["transcription"].iloc[0] == "hello world"
        # file_path dtype -> absolute path with base_audio_path
        assert "wavs" in df["audio_path"].iloc[0]

    def test_no_columns_returns_raw(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.csv", "a,b\n1,2\n")

        schema = DatasetSchema(
            dataset_id="ds",
            format="csv",
            index_file="meta.csv",
        )
        df = IndexLoader(schema, tmp_path).load()
        assert list(df.columns) == ["a", "b"]

    def test_missing_format_uses_index_file_extension(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.csv", "a,b\n1,2\n")
        schema = DatasetSchema(dataset_id="ds", index_file="meta.csv")
        df = IndexLoader(schema, tmp_path).load()
        assert list(df.columns) == ["a", "b"]

    def test_file_path_dtype_resolves_absolute(self, tmp_path: Path) -> None:
        _write(tmp_path / "index.tsv", "path\nclip.mp3\n")

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="index.tsv",
            base_audio_path="clips/",
            columns={"audio": ColumnMapping(source_column="path", dtype="file_path")},
        )
        df = IndexLoader(schema, tmp_path).load()
        expected = str(tmp_path / "clips" / "clip.mp3")
        assert df["audio"].iloc[0] == expected

    def test_file_path_dtype_resolves_absolute_from_relative_extract_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dataset_dir = tmp_path / "dataset"
        _write(dataset_dir / "index.tsv", "path\nclip.mp3\n")

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="index.tsv",
            base_audio_path="clips/",
            columns={"audio": ColumnMapping(source_column="path", dtype="file_path")},
        )

        monkeypatch.chdir(tmp_path)
        df = IndexLoader(schema, Path("dataset")).load()

        assert Path(df["audio"].iloc[0]).is_absolute()
        assert df["audio"].iloc[0] == str(dataset_dir / "clips" / "clip.mp3")

    def test_file_path_uses_first_existing_base_audio_path(
        self, tmp_path: Path
    ) -> None:
        _write(tmp_path / "index.tsv", "path\nclip.wav\n")
        audio_path = tmp_path / "secondary" / "clip.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="index.tsv",
            base_audio_path=["primary/", "secondary/"],
            columns={"audio": ColumnMapping(source_column="path", dtype="file_path")},
        )
        df = IndexLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_exact_search_uses_extension_and_recurses(
        self, tmp_path: Path
    ) -> None:
        _write(tmp_path / "index.tsv", "clip_id\nclip_001\n")
        audio_path = tmp_path / "audio" / "nested" / "clip_001.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
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
        df = IndexLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_contains_search_matches_substring(self, tmp_path: Path) -> None:
        _write(tmp_path / "index.tsv", "clip_fragment\nclip_001\n")
        audio_path = tmp_path / "audio" / "nested" / "speaker_clip_001_take2.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
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
        df = IndexLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_value_already_includes_base_path(self, tmp_path: Path) -> None:
        _write(tmp_path / "index.tsv", "path\ndata/recipes/clip.wav\n")
        audio_path = tmp_path / "data" / "recipes" / "clip.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="index.tsv",
            base_audio_path="data/recipes/",
            columns={"audio": ColumnMapping(source_column="path", dtype="file_path")},
        )
        df = IndexLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_template_builds_name_from_multiple_columns(
        self, tmp_path: Path
    ) -> None:
        _write(
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
        df = IndexLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)

    def test_file_path_template_renders_dynamic_audio_root_from_metadata(
        self, tmp_path: Path
    ) -> None:
        _write(
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
        df = IndexLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(audio_path)
        assert df["text"].iloc[0] == "hello"

    def test_contains_search_raises_on_ambiguous_matches(self, tmp_path: Path) -> None:
        _write(tmp_path / "index.tsv", "clip_fragment\nclip_001\n")
        audio_path_1 = tmp_path / "audio" / "nested" / "speaker_clip_001_take1.wav"
        audio_path_2 = tmp_path / "audio" / "nested" / "speaker_clip_001_take2.wav"
        audio_path_1.parent.mkdir(parents=True, exist_ok=True)
        audio_path_1.write_bytes(b"\x00")
        audio_path_2.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
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
            IndexLoader(schema, tmp_path).load()

    def test_category_dtype(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "i.tsv", "path\tsentence\tspk\nc.mp3\thi\tA\nc2.mp3\tbye\tA\n"
        )

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="i.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
                "speaker": ColumnMapping(source_column="spk", dtype="category"),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert df["speaker"].dtype.name == "category"

    def test_int_and_float_dtypes(self, tmp_path: Path) -> None:
        _write(tmp_path / "i.tsv", "path\tsentence\tdur\tscore\nc.mp3\thi\t100\t0.95\n")

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="i.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
                "duration": ColumnMapping(source_column="dur", dtype="int"),
                "score": ColumnMapping(source_column="score", dtype="float"),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert df["duration"].iloc[0] == 100
        assert df["score"].iloc[0] == pytest.approx(0.95)

    def test_optional_column_missing(self, tmp_path: Path) -> None:
        _write(tmp_path / "i.tsv", "path\tsentence\nc.mp3\thi\n")

        schema = DatasetSchema(
            dataset_id="ds",
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
        df = IndexLoader(schema, tmp_path).load()
        assert "speaker" not in df.columns  # silently skipped

    def test_required_column_missing_raises(self, tmp_path: Path) -> None:
        _write(tmp_path / "i.tsv", "path\tsentence\nc.mp3\thi\n")

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="i.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="nonexistent"),
            },
        )
        with pytest.raises(KeyError, match="nonexistent"):
            IndexLoader(schema, tmp_path).load()

    def test_index_file_not_found_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="missing.tsv",
            columns={"a": ColumnMapping(source_column="x")},
        )
        with pytest.raises(FileNotFoundError, match="missing.tsv"):
            IndexLoader(schema, tmp_path).load()

    def test_explicit_separator_overrides_format(self, tmp_path: Path) -> None:
        _write(tmp_path / "d.csv", "path|sentence\nc.mp3|hi\n")

        schema = DatasetSchema(
            dataset_id="ds",
            format="csv",
            separator="|",
            index_file="d.csv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert len(df) == 1
        assert df["text"].iloc[0] == "hi"

    def test_sniffed_separator_and_trimmed_headers(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "metadata.csv",
            "Topic; Sentence ID ; Sentences \nFood; clip.wav; hello\n",
        )
        (tmp_path / "clip.wav").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            format="csv",
            index_file="metadata.csv",
            columns={
                "audio": ColumnMapping(source_column="Sentence ID", dtype="file_path"),
                "text": ColumnMapping(source_column="Sentences"),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert df["audio"].iloc[0] == str(tmp_path / "clip.wav")
        assert df["text"].iloc[0] == "hello"

    def test_nested_index_file_found(self, tmp_path: Path) -> None:
        """Index file inside a subdirectory should be located via rglob."""
        nested = tmp_path / "sub" / "deep"
        _write(nested / "train.tsv", "path\tsentence\nc.mp3\thi\n")

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="train.tsv",
            columns={
                "audio": ColumnMapping(source_column="path", dtype="file_path"),
                "text": ColumnMapping(source_column="sentence"),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert len(df) == 1

    def test_custom_encoding(self, tmp_path: Path) -> None:
        content = "audio\ttext\nc1.wav\tgrüezi\n"
        (tmp_path / "meta.tsv").write_text(content, encoding="utf-8-sig")

        schema = DatasetSchema(
            dataset_id="ds",
            format="tsv",
            index_file="meta.tsv",
            encoding="utf-8-sig",
            columns={
                "audio": ColumnMapping(source_column="audio", dtype="file_path"),
                "text": ColumnMapping(source_column="text"),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert df["text"].iloc[0] == "grüezi"

    def test_file_content_dtype_reads_text_file(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "index.csv",
            "audio,transcript\nclip.wav,transcripts/clip.txt\n",
        )
        txt_path = tmp_path / "transcripts" / "clip.txt"
        txt_path.parent.mkdir()
        txt_path.write_text("hello world\n", encoding="utf-8")

        schema = DatasetSchema(
            dataset_id="ds",
            format="csv",
            index_file="index.csv",
            columns={
                "audio": ColumnMapping(source_column="audio", dtype="file_path"),
                "text": ColumnMapping(source_column="transcript", dtype="file_content"),
            },
        )
        df = IndexLoader(schema, tmp_path).load()
        assert df["text"].iloc[0] == "hello world"

    def test_file_content_dtype_with_file_extension(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "index.csv",
            "audio,transcript\nclip.wav,transcripts/clip\n",
        )
        txt_path = tmp_path / "transcripts" / "clip.txt"
        txt_path.parent.mkdir()
        txt_path.write_text("resolved with extension\n", encoding="utf-8")

        schema = DatasetSchema(
            dataset_id="ds",
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
        df = IndexLoader(schema, tmp_path).load()
        assert df["text"].iloc[0] == "resolved with extension"
