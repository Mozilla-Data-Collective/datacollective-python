from pathlib import Path

import pandas as pd

SCRIPTED_SPEECH_SPLITS = [
    "dev",
    "train",
    "test",
    "validated",
    "invalidated",
    "reported",
    "other",
]


def _load_scripted(root_dir: Path) -> pd.DataFrame:
    split_files: dict[str, Path] = {}
    for path in root_dir.rglob("*.tsv"):
        split_name = path.stem
        if split_name in SCRIPTED_SPEECH_SPLITS:
            split_files[split_name] = path

    if not split_files:
        raise RuntimeError(f"No scripted split files found under `{str(root_dir)}`")

    frames = []
    for split, file_path in sorted(split_files.items()):
        df = pd.read_csv(file_path, sep="\t", header="infer")
        df["split"] = split
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _load_spontaneous(root_dir: Path) -> pd.DataFrame:
    for path in root_dir.rglob("*.tsv"):
        if path.name.startswith("ss-corpus-"):
            return pd.read_csv(path, sep="\t", header="infer")
    raise RuntimeError(
        f"No spontaneous corpus file (`ss-corpus-*.tsv`) found under `{str(root_dir)}`"
    )
