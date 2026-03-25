import shutil
import tarfile
import zipfile
from pathlib import Path

from datacollective.logging_utils import get_logger

logger = get_logger(__name__)

TAR_GZ_SUFFIX = ".tar.gz"


def _extract_archive(
    archive_path: Path, dest_dir: Path, overwrite_extracted: bool
) -> Path:
    """
    Extract the given archive (.tar.gz, .zip) into `dest_dir`. If the extracted
    directory already exists (check if the default extracted folder exists) and overwrite_extracted is False,
    skip extraction.

    Args:
        archive_path: Path to the archive file.
        dest_dir: Directory where to extract the contents.
        overwrite_extracted: Whether to overwrite existing extracted files.
    Returns:
        Path to the extracted root directory.

    Raises:
        ValueError: If the archive type is unsupported.
    """
    extract_root = _strip_archive_suffix(archive_path)
    # Extract into a dedicated directory under `dest_dir` using stripped name
    target = dest_dir / extract_root.name
    if target.exists():
        if not overwrite_extracted:
            logger.info(
                f"Extracted directory already exists. "
                f"Skipping extraction: `{str(target)}`"
            )
            return target

        logger.info(f"Overwriting existing extracted directory: `{str(target)}`")
        shutil.rmtree(target)

    target.mkdir(parents=True, exist_ok=True)

    if archive_path.suffix == ".zip":
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(target)
    elif archive_path.name.endswith(TAR_GZ_SUFFIX) or archive_path.suffix == ".tgz":
        with tarfile.open(archive_path, "r:gz") as tf:
            tf.extractall(path=target, filter="fully_trusted")
    else:
        raise ValueError(
            f"Unsupported archive type for `{archive_path.name}`. Expected .tar.gz, .tgz, or .zip."
        )
    return target


def _strip_archive_suffix(path: Path) -> Path:
    """
    Strip known archive suffixes from the filename.
    Args:
        path: Path to the archive file.
    Returns:
        Path with the archive suffix removed.
    """
    name = path.name
    if name.endswith(TAR_GZ_SUFFIX):
        return path.with_name(name[: -len(TAR_GZ_SUFFIX)])
    if name.endswith(".tgz"):
        return path.with_name(name[: -len(".tgz")])
    if name.endswith(".zip"):
        return path.with_name(name[: -len(".zip")])
    # Unknown; drop one suffix if present
    return path.with_suffix("")
