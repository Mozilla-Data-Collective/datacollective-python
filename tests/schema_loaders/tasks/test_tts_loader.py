from __future__ import annotations

from pathlib import Path

import pytest

from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.tasks.tts import TTSLoader


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestTTSIndexBased:
    def test_load_pipe_delimited(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.csv", "clip1.mp3|hello world\nclip2.mp3|goodbye\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
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
        df = TTSLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert df["transcription"].iloc[0] == "hello world"
        # file_path dtype -> absolute path with base_audio_path
        assert "wavs" in df["audio_path"].iloc[0]

    def test_load_tsv_with_header(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.tsv", "audio\ttext\nc1.wav\thi\nc2.wav\tbye\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            format="tsv",
            index_file="meta.tsv",
            columns={
                "audio": ColumnMapping(source_column="audio", dtype="file_path"),
                "text": ColumnMapping(source_column="text"),
            },
        )
        df = TTSLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert df["text"].iloc[1] == "bye"

    def test_no_columns_returns_raw(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.csv", "a,b\n1,2\n")

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            format="csv",
            index_file="meta.csv",
        )
        df = TTSLoader(schema, tmp_path).load()
        assert list(df.columns) == ["a", "b"]

    def test_missing_index_file_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(dataset_id="ds", task="TTS")
        with pytest.raises(ValueError, match="index_file"):
            TTSLoader(schema, tmp_path).load()

    def test_missing_format_uses_index_file_extension(self, tmp_path: Path) -> None:
        _write(tmp_path / "meta.csv", "a,b\n1,2\n")
        schema = DatasetSchema(dataset_id="ds", task="TTS", index_file="meta.csv")
        df = TTSLoader(schema, tmp_path).load()
        assert list(df.columns) == ["a", "b"]

    def test_custom_encoding(self, tmp_path: Path) -> None:
        content = "audio\ttext\nc1.wav\tgrüezi\n"
        (tmp_path / "meta.tsv").write_text(content, encoding="utf-8-sig")

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            format="tsv",
            index_file="meta.tsv",
            encoding="utf-8-sig",
            columns={
                "audio": ColumnMapping(source_column="audio", dtype="file_path"),
                "text": ColumnMapping(source_column="text"),
            },
        )
        df = TTSLoader(schema, tmp_path).load()
        assert df["text"].iloc[0] == "grüezi"

    def test_file_path_template_renders_dynamic_audio_root_from_metadata(
        self, tmp_path: Path
    ) -> None:
        _write(
            tmp_path / "dataset" / "metadata.tsv",
            "split\tspeaker_id\tsentence_id\ttext\nrecipes\tspk-01\tsent-01\thello\n",
        )
        audio_path = tmp_path / "dataset" / "recipes" / "spk-01_khm_sent-01.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            format="tsv",
            index_file="metadata.tsv",
            base_audio_path="${split}/",
            columns={
                "audio": ColumnMapping(
                    source_column="sentence_id",
                    dtype="file_path",
                    file_extension=".wav",
                    path_template="${speaker_id}_khm_${value}",
                ),
                "text": ColumnMapping(source_column="text"),
            },
        )
        df = TTSLoader(schema, tmp_path / "dataset").load()
        assert df["audio"].iloc[0] == str(audio_path)
        assert df["text"].iloc[0] == "hello"


class TestTTSPairedGlob:
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
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
        )
        df = TTSLoader(schema, tmp_path).load()
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
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
        )
        df = TTSLoader(schema, tmp_path).load()
        assert len(df) == 1
        assert df["transcription"].iloc[0] == "world"

    def test_paired_glob_no_text_files_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
        )
        with pytest.raises(FileNotFoundError, match="No files matching"):
            TTSLoader(schema, tmp_path).load()

    def test_paired_glob_no_matching_audio_raises(self, tmp_path: Path) -> None:
        """Text files exist but none have matching audio -> error."""
        d = tmp_path / "split"
        d.mkdir()
        _write(d / "001.txt", "hello")

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".webm",
        )
        with pytest.raises(FileNotFoundError, match="No paired"):
            TTSLoader(schema, tmp_path).load()

    def test_paired_glob_missing_file_pattern_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            root_strategy="paired_glob",
            audio_extension=".webm",
        )
        with pytest.raises(ValueError, match="file_pattern"):
            TTSLoader(schema, tmp_path).load()

    def test_paired_glob_missing_audio_extension_raises(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
        )
        with pytest.raises(ValueError, match="audio_extension"):
            TTSLoader(schema, tmp_path).load()

    def test_paired_glob_reads_transcription_stripped(self, tmp_path: Path) -> None:
        d = tmp_path / "s"
        d.mkdir()
        _write(d / "001.txt", "  hello world  \n")
        (d / "001.wav").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".wav",
        )
        df = TTSLoader(schema, tmp_path).load()
        assert df["transcription"].iloc[0] == "hello world"

    def test_paired_glob_audio_path_is_absolute(self, tmp_path: Path) -> None:
        d = tmp_path / "s"
        d.mkdir()
        _write(d / "001.txt", "hi")
        (d / "001.wav").write_bytes(b"\x00")

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".wav",
        )
        df = TTSLoader(schema, tmp_path).load()
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
            task="TTS",
            root_strategy="paired_glob",
            file_pattern="**/*.txt",
            audio_extension=".wav",
        )

        monkeypatch.chdir(tmp_path)
        df = TTSLoader(schema, Path("dataset")).load()

        assert Path(df["audio_path"].iloc[0]).is_absolute()
        assert df["audio_path"].iloc[0] == str(dataset_dir / "s" / "001.wav")


class TestTTSMultiSections:
    def _setup_sections(self, root: Path, sections: list[str]) -> None:
        """Create a multi-sections dataset structure under root."""
        for section in sections:
            _write(
                root / "dataset" / section / "metadata.tsv",
                f"audio\ttext\n{section.lower()}.wav\tHello from {section}\n",
            )

    def test_load_multiple_sections(self, tmp_path: Path) -> None:
        self._setup_sections(tmp_path, ["General", "Chat"])

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            root_strategy="multi_sections",
            section_root="dataset",
            sections=["General", "Chat"],
            index_file="metadata.tsv",
            format="tsv",
        )
        df = TTSLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert "section" in df.columns
        assert set(df["section"]) == {"General", "Chat"}
        assert set(df["text"]) == {"Hello from General", "Hello from Chat"}

    def test_multi_sections_ignores_unlisted_sections(self, tmp_path: Path) -> None:
        self._setup_sections(tmp_path, ["General", "Chat", "Other"])

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            root_strategy="multi_sections",
            section_root="dataset",
            sections=["General", "Chat"],
            index_file="metadata.tsv",
            format="tsv",
        )
        df = TTSLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert set(df["section"]) == {"General", "Chat"}

    def test_multi_sections_missing_index_file_raises(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "dataset" / "General" / "metadata.tsv",
            "audio\ttext\ngeneral.wav\tHello from General\n",
        )

        schema = DatasetSchema(
            dataset_id="ds",
            task="TTS",
            root_strategy="multi_sections",
            section_root="dataset",
            sections=["General", "Chat"],
            index_file="metadata.tsv",
            format="tsv",
        )
        with pytest.raises(FileNotFoundError, match="Chat"):
            TTSLoader(schema, tmp_path).load()
