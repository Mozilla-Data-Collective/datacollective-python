import shutil
import tarfile
from pathlib import Path

from datacollective.logging_utils import get_logger

logger = get_logger(__name__)

#: Archive suffixes accepted by the platform for dataset uploads.
TAR_GZ_SUFFIXES = (".tar.gz", ".tgz")


def _extract_archive(
    archive_path: Path, dest_dir: Path, overwrite_extracted: bool
) -> Path:
    """
    Extract the given `.tar.gz` / `.tgz` archive into `dest_dir`. If the extracted
    directory already exists and overwrite_extracted is False, skip extraction.

    Args:
        archive_path: Path to the archive file.
        dest_dir: Directory where to extract the contents.
        overwrite_extracted: Whether to overwrite existing extracted files.
    Returns:
        Path to the extracted root directory.

    Raises:
        ValueError: If the archive is not a `.tar.gz` / `.tgz` file.
    """
    suffix = next((s for s in TAR_GZ_SUFFIXES if archive_path.name.endswith(s)), None)
    if suffix is None:
        raise ValueError(
            f"Unsupported archive type for `{archive_path.name}`. "
            f"Expected {' or '.join(TAR_GZ_SUFFIXES)}."
        )

    # Extract into a dedicated directory under `dest_dir` named after the archive
    target = dest_dir / archive_path.name.removesuffix(suffix)
    if target.exists():
        if not overwrite_extracted:
            logger.info(
                f"Extracted directory already exists. Skipping extraction: `{target}`"
            )
            return target

        logger.info(f"Overwriting existing extracted directory: `{target}`")
        shutil.rmtree(target)

    target.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:gz") as tf:
        tf.extractall(path=target, filter="data")
    return target
