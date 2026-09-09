from __future__ import annotations

from pathlib import Path

import pytest

from datacollective.schema import ColumnMapping, DatasetSchema
from datacollective.schema_loaders.strategies.multi_sections import MultiSectionsLoader


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _setup_sections(root: Path, sections: list[str]) -> None:
    """Create a multi-sections dataset structure under root."""
    for section in sections:
        _write(
            root / "dataset" / section / "metadata.tsv",
            f"audio\ttext\n{section.lower()}.wav\tHello from {section}\n",
        )


class TestMultiSectionsValidation:
    def test_requires_sections(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_sections",
            section_root="dataset",
            index_file="metadata.tsv",
        )
        with pytest.raises(ValueError, match="sections"):
            MultiSectionsLoader(schema, tmp_path)

    def test_requires_section_root(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_sections",
            sections=["General"],
            index_file="metadata.tsv",
        )
        with pytest.raises(ValueError, match="section_root"):
            MultiSectionsLoader(schema, tmp_path)

    def test_requires_index_file(self, tmp_path: Path) -> None:
        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_sections",
            sections=["General"],
            section_root="dataset",
        )
        with pytest.raises(ValueError, match="index_file"):
            MultiSectionsLoader(schema, tmp_path)


class TestMultiSectionsLoader:
    def test_load_multiple_sections(self, tmp_path: Path) -> None:
        _setup_sections(tmp_path, ["General", "Chat"])

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_sections",
            section_root="dataset",
            sections=["General", "Chat"],
            index_file="metadata.tsv",
            format="tsv",
        )
        df = MultiSectionsLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert "section" in df.columns
        assert set(df["section"]) == {"General", "Chat"}
        assert set(df["text"]) == {"Hello from General", "Hello from Chat"}

    def test_multi_sections_ignores_unlisted_sections(self, tmp_path: Path) -> None:
        _setup_sections(tmp_path, ["General", "Chat", "Other"])

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_sections",
            section_root="dataset",
            sections=["General", "Chat"],
            index_file="metadata.tsv",
            format="tsv",
        )
        df = MultiSectionsLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert set(df["section"]) == {"General", "Chat"}

    def test_multi_sections_missing_index_file_raises(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "dataset" / "General" / "metadata.tsv",
            "audio\ttext\ngeneral.wav\tHello from General\n",
        )

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_sections",
            section_root="dataset",
            sections=["General", "Chat"],
            index_file="metadata.tsv",
            format="tsv",
        )
        with pytest.raises(FileNotFoundError, match="Chat"):
            MultiSectionsLoader(schema, tmp_path).load()

    def test_multi_sections_applies_column_mappings(self, tmp_path: Path) -> None:
        """Declared column mappings are applied and the section column is kept."""
        _setup_sections(tmp_path, ["General", "Chat"])

        schema = DatasetSchema(
            dataset_id="ds",
            root_strategy="multi_sections",
            section_root="dataset",
            sections=["General", "Chat"],
            index_file="metadata.tsv",
            format="tsv",
            columns={
                "audio_path": ColumnMapping(source_column="audio", dtype="file_path"),
                "transcription": ColumnMapping(source_column="text"),
            },
        )
        df = MultiSectionsLoader(schema, tmp_path).load()
        assert len(df) == 2
        assert list(df.columns) == ["audio_path", "transcription", "section"]
        assert set(df["section"]) == {"General", "Chat"}
        assert set(df["transcription"]) == {"Hello from General", "Hello from Chat"}
