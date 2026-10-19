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

    The archive is extracted under a temporary name and renamed once complete, so an
    interrupted extraction is never mistaken for a finished one. When overwriting, the
    existing directory is replaced only after the new extraction has succeeded, so a bad
    archive never costs the user a good directory. A failed extraction cleans up after
    itself.

    Args:
        archive_path: Path to the archive file.
        dest_dir: Directory where to extract the contents.
        overwrite_extracted: Whether to overwrite existing extracted files.
    Returns:
        Path to the extracted root directory.

    Raises:
        ValueError: If the archive is not a `.tar.gz` / `.tgz` file.
        FileExistsError: If something other than a directory sits where the archive
            would be extracted. It is never deleted, even with `overwrite_extracted`.
    """
    dir_name = _extracted_dir_name(archive_path.name)
    if dir_name is None:
        raise ValueError(
            f"Unsupported archive type for `{archive_path.name}`. "
            f"Expected {' or '.join(TAR_GZ_SUFFIXES)}."
        )

    # Extract into a dedicated directory under `dest_dir` named after the archive
    target = dest_dir / dir_name
    partial = target.with_name(target.name + ".partial")
    # Only directories this function created may be reused or removed. A stray file in
    # the way cannot be the extracted dataset, and deleting it would destroy user data.
    for path in (target, partial):
        if path.exists() and not path.is_dir():
            raise FileExistsError(
                f"Cannot extract `{archive_path.name}`: `{path}` exists and is not a "
                "directory. Move or remove it first."
            )

    if target.exists() and not overwrite_extracted:
        logger.info(
            f"Extracted directory already exists. Skipping extraction: `{target}`"
        )
        return target

    # Left over from an interrupted extraction; start again.
    if partial.exists():
        shutil.rmtree(partial)

    partial.mkdir(parents=True)
    try:
        with tarfile.open(archive_path, "r:gz") as tf:
            tf.extractall(path=partial, filter="data")
        # Only now, with a complete extraction in hand, is the old directory given up.
        if target.exists():
            logger.info(f"Overwriting existing extracted directory: `{target}`")
            shutil.rmtree(target)
        partial.rename(target)
    except BaseException:
        shutil.rmtree(partial, ignore_errors=True)
        raise
    return target


def _extracted_dir_name(archive_name: str) -> str | None:
    """Name of the directory an archive extracts into: its name without the archive
    suffix. None if it is not a supported archive."""
    suffix = next((s for s in TAR_GZ_SUFFIXES if archive_name.endswith(s)), None)
    if suffix is None or archive_name == suffix:
        return None
    return archive_name.removesuffix(suffix)
