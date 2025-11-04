from pathlib import Path

import pandas as pd
from datacollective.dataset_loading_scripts.common_voice import (
    _load_scripted,
    _load_spontaneous,
)


def load_dataset_from_name_as_dataframe(
    dataset_name: str, extract_dir: Path
) -> pd.DataFrame:
    if "scripted" in dataset_name:
        return _load_scripted(extract_dir)
    if "spontaneous" in dataset_name:
        return _load_spontaneous(extract_dir)

    raise ValueError(
        f"Dataset name `{dataset_name}` currently not supported for loading as DataFrame."
    )
