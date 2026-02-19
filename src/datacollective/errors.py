from datacollective.api_utils import _format_bytes

class DownloadError(Exception):
    """Exception raised when a download fails."""

    def __init__(
        self,
        session_bytes: int,
        total_downloaded_bytes: int,
        total_archive_bytes: int,
        checksum: str | None,
    ):
        remaining_bytes = total_archive_bytes - total_downloaded_bytes
        self.remaining_percentage = round(
            (
                (remaining_bytes / total_archive_bytes) * 100
                if total_archive_bytes > 0
                else 0
            ),
            2,
        )
        self.remaining_bytes = _format_bytes(remaining_bytes)
        self.session_bytes = _format_bytes(session_bytes)
        self.total_downloaded_bytes = _format_bytes(total_downloaded_bytes)
        self.total_archive_bytes = _format_bytes(total_archive_bytes)
        self.checksum = checksum
        super().__init__()

    def __str__(self) -> str:
        if self.checksum:
            return f"""Download failed with {self.session_bytes} bytes written in this session.
            You can try downloading again to resume the session.
            Dataset Download Details:
            - Bytes downloaded in this session: {self.session_bytes}
            - Bytes downloaded in total: {self.total_downloaded_bytes}
            - Bytes remaining: {self.remaining_bytes} ({self.remaining_percentage}% remaining)
            - Total archive size: {self.total_archive_bytes}
            """
        return "Download failed. Unfortunately this dataset does not support resuming downloads — please try again."
