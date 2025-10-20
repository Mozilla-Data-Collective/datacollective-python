import os
import pandas as pd

class Dataset():

    def __init__(self, directory: str):

        self.directory = directory
        self.splits = self.get_possible_splits()

    
    def get_possible_splits(self):

        data_files: dict[str, str] = {}
        for root, _, files in os.walk(self.directory):
            for file in files:
                if not file.endswith(".tsv"):
                    continue
                
                full_path = os.path.join(root, file)
                data_file_name = file[:-4]
                data_files[data_file_name] = full_path
        
        return data_files
    

    def split(self, split: str):
        if (split_file := self.splits.get(split)):
            return DatasetSplit(split_file)
        else:
            raise Exception(f"Split named {split} not found in dataset")


class DatasetSplit():

    def __init__(self, split_file: str):
        self.split_file = split_file


    def to_pandas(self):
        return pd.read_csv(self.split_file, sep="\t", header='infer')
