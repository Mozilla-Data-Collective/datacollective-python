"""
Mozilla Data Collective Python Client Library
"""

from .datasets import get_dataset_details, load_dataset, save_dataset_to_disk

__all__ = ["save_dataset_to_disk", "load_dataset", "get_dataset_details", "__version__"]

# DO NOT EDIT THE VERSION MANUALLY, USE bump-my-version TO UPDATE. See release.md
__version__ = "0.2.0"
