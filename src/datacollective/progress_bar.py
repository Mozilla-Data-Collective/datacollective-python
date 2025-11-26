import sys
import time

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

