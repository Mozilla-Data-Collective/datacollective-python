import os

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


class Dataset:
    """
    Represents a dataset. Should be the jumping off point to access its data, metadata, anything that comes from it.
    A dataset is backed by a directory, that contains all of its data.
    """

    def __init__(self, directory: str):
        self.directory = directory

    @property
    def splits(self) -> list[str]:
        """
        A list of splits available for the dataset
        """
        return [str(x) for x in self._data["split"].dropna().unique().tolist()]

    @property
    def _data(self) -> pd.DataFrame:
        """
        A single opinion of how a dataset's data should be presented
        A table of all splits in a dataset, can be differentiated via the split column
        """

        if "/mcv-scripted-" in self.directory:
            return self._get_scripted_speech_data()
        elif "/mcv-spontaneous-" in self.directory:
            return self._get_spontaneous_speech_data()
        else:
            raise Exception(
                f"Dataset directory {self.directory} cannot be identified as MCV scripted or spontaneous"
            )

    def _get_scripted_speech_data(self) -> pd.DataFrame:
        """
        A crude method of getting all of the data for a scripted speech dataset
        Transforms it into the canonical representation of several splits of data
        In the future, we will aim for a more robust solution
        """
        split_files: dict[str, str] = {}
        for root, _, files in os.walk(self.directory):
            for file in files:
                if not file.endswith(".tsv"):
                    continue

                full_path = os.path.join(root, file)
                data_file_name = file[:-4]
                if data_file_name not in SCRIPTED_SPEECH_SPLITS:
                    continue

                split_files[data_file_name] = full_path

        dfs = []
        for split, file in split_files.items():
            df = pd.read_csv(file, sep="\t", header="infer")
            df["split"] = split
            dfs.append(df)

        return pd.concat(dfs, ignore_index=True)

    def _get_spontaneous_speech_data(self) -> pd.DataFrame:
        """
        A crude method of getting all of the data for a spontaneous speech dataset
        Transforms it into the canonical representation of several splits of data
        In the future, we will aim for a more robust solution
        """

        for root, _, files in os.walk(self.directory):
            for file in files:
                if not file.startswith("ss-corpus-"):
                    continue

                if not file.endswith(".tsv"):
                    continue

                full_path = os.path.join(root, file)
                return pd.read_csv(full_path, sep="\t", header="infer")

        raise Exception("Could nof find dataset file in directory")

    # This may look redundant today, but this is intentionally designed to present an API which is agnostic to its own insides.
    # The inside might be anything, you call this to know you've got pandas
    def to_pandas(self) -> pd.DataFrame:
        """
        Provides the dataset in a pandas format.
        """
        return self._data
