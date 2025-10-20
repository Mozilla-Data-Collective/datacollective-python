import os

from abc import ABC, abstractmethod

import pandas as pd

SCRIPTED_SPEECH_SPLITS = ["dev", "train", "test", "validated", "invalidated", "reported", "other"]



class Dataset():

    def __init__(self, directory: str):
        self.directory = directory

    @property
    def splits_list(self):
        return [str(x) for x in self.data["split"].dropna().unique().tolist()]

    @property
    def _data(self):

        if self.directory.startswith("mcv-scripted-"):
            return self.get_scripted_speech_splits()
        elif self.directory.startswith("mcv-spontaneous-"):
            return self.get_spontaneous_speech_splits()
        else:
            raise Exception("Dataset cannot be identified as MCV scripted or spontaneous")

    
    def get_scripted_speech_splits(self):
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
            df = pd.read_csv(file, sep="\t", header='infer')
            df["split"] = split
            dfs.append(df)
        
        return pd.concat(dfs, ignore_index=True)
    
    def get_spontaneous_speech_splits(self):

        for root, _, files in os.walk(self.directory):
            for file in files:
                if not file.startswith("ss-corpus-"):
                    continue

                if not file.endswith(".tsv"):
                    continue
                
                full_path = os.path.join(root, file)
                return pd.read_csv(full_path, sep="\t", header='infer')
        
        raise Exception("Could nof find dataset file in directory")
        

    def to_pandas(self):
        return self.data