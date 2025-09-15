import os
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv


class DataCollective:

    def __init__(
        self,
        api_key: Optional[str] = None,
        environment: str = "production",
        download_path: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the DataCollective client object
        """

        env = environment or os.getenv("ENVIRONMENT", "development")
        env_file = f".env.{env}" if env != "production" else ".env"

        if os.path.exists(env_file):
            load_dotenv(
                dotenv_path=env_file
            )  # load in environmental specific .env file
        else:
            load_dotenv()  # load in default .env file

        # set up API URL
        self.api_url = (
            os.getenv("MDC_API_URL")
            or "https://datacollective.mozillafoundation.org/api"
        )
        if not self.api_url.endswith("/"):
            self.api_url += "/"  # add trailing slash if it isn't already included

        # set up API Key
        self.api_key = api_key or os.getenv("MDC_API_KEY")

        if not self.api_key:
            raise ValueError(
                "API key missing. Please provide one when creating this object with the api_key parameter or provide it in your .env file as MDC_API_KEY"
            )

        # set up download path
        download_path_env = download_path or os.getenv(
            "MDC_DOWNLOAD_PATH", "~/.mozdata/datasets"
        )
        # Expand user path (handle ~)
        self.download_path = os.path.expanduser(download_path_env)  # type: ignore

    def _ensure_download_directory(self, download_path: str) -> None:
        """
        Ensure the download directory exists and is writable.
        Raises an error if the directory cannot be created or is not writable.
        """
        try:
            # Create the directory if it doesn't exist
            Path(download_path).mkdir(parents=True, exist_ok=True)

            # Check if the directory is writable
            if not os.access(download_path, os.W_OK):
                raise PermissionError(f"Directory {download_path} is not writable")

        except PermissionError as e:
            raise PermissionError(
                f"Cannot create or write to directory {download_path}: {e}"
            ) from e
        except Exception as e:
            raise OSError(f"Failed to create directory {download_path}: {e}") from e

    def get_dataset(
        self, dataset: str, download_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Download a dataset from the DataCollective API.

        Args:
            dataset (str): The name/ID of the dataset to download
            download_path (str, optional): Override the default download path for this download

        Returns:
            str: The full path to the downloaded file, or None if download failed
        """

        # Determine the download path for this download
        if download_path is not None:
            # Expand user path (handle ~)
            final_download_path = os.path.expanduser(download_path)
        else:
            final_download_path = self.download_path  # type: ignore

        # Ensure the download directory exists and is writable
        self._ensure_download_directory(final_download_path)

        # create a download session
        download_session_url = self.api_url + "datasets/" + dataset + "/download"
        headers = {"Authorization": "Bearer " + self.api_key}  # type: ignore

        try:
            r = requests.post(download_session_url, headers=headers)
            r.raise_for_status()
            # parse response once
            response_data = r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # rate limit exceeded
                print("Rate limit exceeded")
                return None
            print(f"HTTP Error: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            return None

        if "error" in response_data:
            response_error = response_data["error"]
            if response_error == "Rate limit exceeded":
                print("Rate limit exceeded")
                return None
            else:
                print(f"API Error: {response_error}")
                return None

        if "downloadUrl" not in response_data or "filename" not in response_data:
            print(f"Unexpected response format: {response_data}")

        dataset_file_url = response_data["downloadUrl"]
        dataset_filename = response_data["filename"]

        # download dataset file
        try:
            headers = {"Authorization": "Bearer " + self.api_key}  # type: ignore
            r = requests.get(
                dataset_file_url, stream=True, headers=headers
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error Downloading File: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request Error Downloading File: {e}")
            return None

        # Create the full file path
        full_file_path = os.path.join(final_download_path, dataset_filename)

        print(f"Downloading dataset: {dataset_filename}")
        with open(full_file_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Dataset downloaded to: {full_file_path}")
        return full_file_path
