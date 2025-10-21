import pandas as pd
import pytest

from datacollective.dataset import SCRIPTED_SPEECH_SPLITS, Dataset


@pytest.fixture
def scripted_dataset_dir(tmp_path):
    """Create a fake MCV scripted dataset directory with valid .tsv split files."""
    base_dir = tmp_path / "mcv-scripted-en"
    base_dir.mkdir()
    for split in ["train", "test", "validated"]:
        df = pd.DataFrame({"text": [f"{split}_1", f"{split}_2"], "speaker": [1, 2]})
        file_path = base_dir / f"{split}.tsv"
        df.to_csv(file_path, sep="\t", index=False)
    return base_dir


@pytest.fixture
def spontaneous_dataset_dir(tmp_path):
    """Create a fake MCV spontaneous dataset directory with one ss-corpus file."""
    base_dir = tmp_path / "mcv-spontaneous-en"
    base_dir.mkdir()
    df = pd.DataFrame({"utterance": ["hello", "world"], "speaker": [1, 2]})
    (base_dir / "ss-corpus-data.tsv").write_text(df.to_csv(sep="\t", index=False))
    return base_dir


def test_scripted_dataset_loads_correctly(scripted_dataset_dir):
    ds = Dataset(str(scripted_dataset_dir))
    df = ds.to_pandas()

    # Should contain concatenated data from all splits
    assert set(df["split"].unique()) == {"train", "test", "validated"}
    assert all(col in df.columns for col in ["text", "speaker", "split"])
    assert len(df) == 6  # 3 splits × 2 rows each


def test_scripted_splits_property(scripted_dataset_dir):
    ds = Dataset(str(scripted_dataset_dir))
    splits = ds.splits
    assert sorted(splits) == ["test", "train", "validated"]


def test_spontaneous_dataset_loads_correctly(spontaneous_dataset_dir):
    ds = Dataset(str(spontaneous_dataset_dir))
    df = ds.to_pandas()

    assert set(df.columns) == {"utterance", "speaker"}
    assert len(df) == 2
    assert df.iloc[0]["utterance"] == "hello"


def test_spontaneous_dataset_missing_file_raises(tmp_path):
    base_dir = tmp_path / "mcv-spontaneous-en"
    base_dir.mkdir()

    ds = Dataset(str(base_dir))
    with pytest.raises(Exception, match="Could nof find dataset file in directory"):
        ds.to_pandas()


def test_invalid_dataset_dir_raises(tmp_path):
    base_dir = tmp_path / "some-random-dataset"
    base_dir.mkdir()
    ds = Dataset(str(base_dir))

    with pytest.raises(
        Exception, match="cannot be identified as MCV scripted or spontaneous"
    ):
        ds.to_pandas()


def test_get_scripted_speech_splits_filters_only_valid_names(tmp_path):
    base_dir = tmp_path / "mcv-scripted-en"
    base_dir.mkdir()
    # valid and invalid split names
    valid_file = base_dir / "train.tsv"
    invalid_file = base_dir / "random.tsv"

    pd.DataFrame({"x": [1]}).to_csv(valid_file, sep="\t", index=False)
    pd.DataFrame({"x": [1]}).to_csv(invalid_file, sep="\t", index=False)

    ds = Dataset(str(base_dir))
    df = ds._get_scripted_speech_data()

    assert "split" in df.columns
    assert all(df["split"].isin(SCRIPTED_SPEECH_SPLITS))
    assert "random" not in df["split"].unique()
