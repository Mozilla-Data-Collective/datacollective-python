import os
import shutil
import sys
import tarfile
import time
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv

from .dataset import Dataset


class ProgressBar:
    """A custom progress bar with a fox emoji that moves across the bar"""

    def __init__(self, total_size: int, bar_length: int = 50):
        self.total_size = total_size
        self.downloaded = 0
        self.bar_length = bar_length
        self.start_time = time.time()
        self.last_update_time = 0.0
        self.update_interval = 0.1  # Update every 100ms max

    def update(self, chunk_size: int) -> None:
        """Update the progress bar with new downloaded data"""
        self.downloaded += chunk_size

        # Only update display if enough time has passed
        current_time = time.time()
        if current_time - self.last_update_time >= self.update_interval:
            self._display()
            self.last_update_time = current_time

    def _display(self) -> None:
        """Display the current progress bar"""
        if self.total_size <= 0:
            # If we don't know the total size, show a spinning fox
            spinner = ["🦊", "🦊", "🦊", "🦊"]
            spin_char = spinner[int(time.time() * 2) % len(spinner)]
            sys.stdout.write(
                f"\r{spin_char} Downloading... {self._format_bytes(self.downloaded)}"
            )
            sys.stdout.flush()
            return

        # Calculate percentage and bar position
        percentage = min(100.0, (self.downloaded / self.total_size) * 100)
        filled_length = int(self.bar_length * self.downloaded // self.total_size)

        # Create the progress bar - always show fox, even at 0%
        bar = "█" * filled_length + "░" * (self.bar_length - filled_length)

        # Position the fox emoji - always visible at position 0 or current progress
        if filled_length == 0:
            # Fox at the start when no progress yet
            bar = "🦊" + bar[1:]
        else:
            # Fox at the leading edge of progress
            fox_position = min(filled_length, self.bar_length - 1)
            bar = bar[:fox_position] + "🦊" + bar[fox_position + 1 :]

        # Calculate speed and ETA
        elapsed_time = time.time() - self.start_time
        if elapsed_time > 0 and self.downloaded > 0:
            speed = self.downloaded / elapsed_time
            eta = (self.total_size - self.downloaded) / speed if speed > 0 else 0
            speed_str = f"{self._format_bytes(speed)}/s"
            eta_str = f"ETA: {self._format_time(eta)}"
        else:
            speed_str = "0 B/s"
            eta_str = "ETA: --:--"

        # Display the progress bar
        sys.stdout.write(
            f"\r{bar} {percentage:.1f}% "
            f"({self._format_bytes(self.downloaded)}/{self._format_bytes(self.total_size)}) "
            f"{speed_str} {eta_str}"
        )
        sys.stdout.flush()

    def finish(self) -> None:
        """Complete the progress bar and move to next line"""
        if self.total_size > 0:
            # Show completed bar with fox at the end
            bar = "█" * (self.bar_length - 1) + "🦊"
            elapsed_time = time.time() - self.start_time
            avg_speed = self.downloaded / elapsed_time if elapsed_time > 0 else 0
            sys.stdout.write(
                f"\r{bar} 100.0% "
                f"({self._format_bytes(self.downloaded)}/{self._format_bytes(self.total_size)}) "
                f"Average: {self._format_bytes(avg_speed)}/s "
                f"Total time: {self._format_time(elapsed_time)}\n"
            )
        else:
            elapsed_time = time.time() - self.start_time
            avg_speed = self.downloaded / elapsed_time if elapsed_time > 0 else 0
            sys.stdout.write(
                f"\n🦊 Download complete! {self._format_bytes(self.downloaded)} "
                f"in {self._format_time(elapsed_time)} "
                f"(avg: {self._format_bytes(avg_speed)}/s)\n"
            )
        sys.stdout.flush()

    @staticmethod
    def _format_bytes(bytes_val: float) -> str:
        """Format bytes into human readable format"""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_val < 1024.0:
                return f"{bytes_val:.1f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.1f} PB"

    @staticmethod
    def _format_time(seconds: float) -> str:
        """Format seconds into MM:SS format"""
        if seconds < 0:
            return "--:--"
        mins, secs = divmod(int(seconds), 60)
        return f"{mins:02d}:{secs:02d}"


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
        self,
        dataset: str,
        download_path: Optional[str] = None,
        show_progress: bool = True,
    ) -> Optional[str]:
        """
        Download a dataset from the DataCollective API.

        Args:
            dataset (str): The name/ID of the dataset to download
            download_path (str, optional): Override the default download path for this download
            show_progress (bool): Whether to show the progress bar (default: True)

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

        print(f"Requesting dataset: {dataset}")
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
            r = requests.get(dataset_file_url, stream=True, headers=headers)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error Downloading File: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request Error Downloading File: {e}")
            return None

        # Create the full file path
        full_file_path = os.path.join(final_download_path, dataset_filename)

        # Get the total file size for the progress bar
        total_size = int(r.headers.get("content-length", 0))

        if show_progress:
            print(f"Downloading dataset: {dataset_filename}")
            progress_bar = ProgressBar(total_size)
            # Show initial progress bar with fox at the start
            progress_bar._display()
        else:
            print(f"Downloading dataset: {dataset_filename}")

        # Download with progress tracking
        with open(full_file_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):  # Increased chunk size
                if chunk:
                    f.write(chunk)
                    if show_progress:
                        progress_bar.update(len(chunk))

        if show_progress:
            progress_bar.finish()

        print(f"Dataset downloaded to: {full_file_path}")
        return full_file_path

    def load_dataset(self, dataset: str) -> Dataset:

        filepath = self.get_dataset(dataset)
        if not filepath:
            raise Exception("Downloading dataset failed")

        extract_path = self._extract_dataset(filepath)
        return Dataset(extract_path)

    def _extract_dataset(self, filepath: str) -> str:

        archive_suffix = ".tar.gz"
        if filepath.endswith(archive_suffix):
            extract_path = filepath[: -len(archive_suffix)]
        else:
            raise Exception(
                f"Downloaded archive {filepath} does not end with {archive_suffix}"
            )

        if os.path.exists(extract_path):
            print(f"Deleting old extract {extract_path}")
            shutil.rmtree(extract_path)

        print(f"Extracting {filepath} to {extract_path}")
        with tarfile.open(filepath, "r:gz") as tar:
            tar.extractall(path=extract_path)
        print(f"Extracted {filepath} to {extract_path}")
        return extract_path
