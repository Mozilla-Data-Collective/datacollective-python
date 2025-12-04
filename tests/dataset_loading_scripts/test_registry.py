from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from datacollective.dataset_loading_scripts import registry


def test_load_dataset_routes_to_scripted(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    sentinel = object()

    def fake_loader(extract_dir: Path) -> object:
        assert extract_dir == tmp_path
        return sentinel

    monkeypatch.setattr(registry, "_load_scripted", fake_loader)
    result = registry.load_dataset_from_name_as_dataframe(
        "common voice scripted", tmp_path
    )
    assert result is sentinel


def test_load_dataset_routes_to_spontaneous(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    sentinel = object()

    def fake_loader(extract_dir: Path) -> object:
        assert extract_dir == tmp_path
        return sentinel

    monkeypatch.setattr(registry, "_load_spontaneous", fake_loader)
    result = registry.load_dataset_from_name_as_dataframe(
        "common voice spontaneous", tmp_path
    )
    assert result is sentinel


def test_load_dataset_invalid_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        registry.load_dataset_from_name_as_dataframe("unknown dataset", tmp_path)
