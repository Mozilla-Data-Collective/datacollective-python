import sys
import time

from fox_progress_bar import ProgressBar


class _ProgressBar(ProgressBar):
    """Keep fox-progress-bar's accounting, but render safely to interactive stderr.

    The dependency hardcodes stdout in both rendering methods.
    """

    def _write(self, text: str) -> None:
        stream = sys.stderr
        if stream is None or not stream.isatty():
            return
        encoding = getattr(stream, "encoding", None) or "ascii"
        stream.write(text.encode(encoding, errors="replace").decode(encoding))
        stream.flush()

    def _display(self) -> None:
        if self.total_size <= 0:
            return

        percentage = min(100.0, self.downloaded / self.total_size * 100)
        filled_length = int(self.bar_length * self.downloaded // self.total_size)
        bar = "█" * filled_length + "░" * (self.bar_length - filled_length)
        fox_position = min(filled_length, self.bar_length - 1)
        bar = bar[:fox_position] + "🦊" + bar[fox_position + 1 :]

        elapsed_time = time.time() - self.start_time
        if elapsed_time > 0 and self.downloaded > 0:
            speed = self.downloaded / elapsed_time
            eta = (self.total_size - self.downloaded) / speed
            speed_str = f"{self._format_bytes(speed)}/s"
            eta_str = f"ETA: {self._format_time(eta)}"
        else:
            speed_str = "0.0 B/s"
            eta_str = "ETA: --:--"

        self._write(
            f"\r{bar} {percentage:.1f}% "
            f"({self._format_bytes(self.downloaded)}/{self._format_bytes(self.total_size)}) "
            f"{speed_str} {eta_str}"
        )

    def finish(self) -> None:
        elapsed_time = time.time() - self.start_time
        avg_speed = self.downloaded / elapsed_time if elapsed_time > 0 else 0
        if self.total_size > 0:
            bar = "█" * (self.bar_length - 1) + "🦊"
            self._write(
                f"\r{bar} 100.0% "
                f"({self._format_bytes(self.downloaded)}/{self._format_bytes(self.total_size)}) "
                f"Average: {self._format_bytes(avg_speed)}/s "
                f"Total time: {self._format_time(elapsed_time)}\n"
            )
        else:
            self._write(
                f"\n🦊 Download complete! {self._format_bytes(self.downloaded)} "
                f"in {self._format_time(elapsed_time)} "
                f"(avg: {self._format_bytes(avg_speed)}/s)\n"
            )
