import tarfile
import json
import datasets

from .client import DataCollective

class DatasetsWrapper:
    """Wraps some functions of the datasets api to smoothly integrate with MDC hosted datasets"""

    def __init__(self, client: DataCollective):
        self.__client = client

    
    def load_dataset(self, dataset: str, **kwargs):

        # We should consider local caching
        # in case people want to try loading the dataset under different params
        download_path = self.__client.get_dataset(dataset)
        
        archive_suffix = ".tar.gz"
        if download_path.endswith(archive_suffix):
            extract_path = download_path[:-len(archive_suffix)]
        else:
            raise Exception(f"Downloaded archive {download_path} does not end with {archive_suffix}")
        
        print(f"Extracting {download_path} to {extract_path}")
        with tarfile.open(download_path, 'r:gz') as tar:
            tar.extractall(path=extract_path)
        print(f"Extracted {download_path} to {extract_path}")

        # Normally, we would want dataset providers to provide their own configuration 
        #
        # datasets_config_path = f"{extract_path}/datasets_config.json"
        # with open(datasets_config_path, "r") as f:
        #     datasets_config = json.load(f)
        # datasets_config = {
        #     "data_files": datasets_config["data_files"] # Limit what params a config can change
        # }

        datasets_config = {
            "data_files": "cv-corpus-23.0-2025-09-05/bas/train.tsv"
        }

        datasets_config.update(kwargs) # Override with any configuration the user sets

        return datasets.load_dataset(path=extract_path, **datasets_config)
