
class DownloadError(Exception):
    """Exception raised when a download fails."""

    def __init__(
        self,
        downloaded_bytes: int,
        checksum: str | None,
    ):
        self.downloaded_bytes = downloaded_bytes
        self.checksum = checksum
        super().__init__()

    def __str__(self) -> str:
        if self.checksum:
            return f"""Download failed with {self.downloaded_bytes} bytes written. Run again with resume_download="{self.checksum}" to resume."""
        return "Download failed. Unfortunately this dataset does not support resuming downloads — please try again."
        