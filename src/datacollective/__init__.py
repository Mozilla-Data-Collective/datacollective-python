"""Mozilla Data Collective Python Client Library."""

from datacollective.datasets import (
    download_dataset,
    get_dataset_details,
    list_dataset_filters,
    list_datasets,
    load_dataset,
    save_dataset_to_disk,
)
from datacollective.errors import (
    AuthenticationError,
    RateLimitError,
)
from datacollective.models import (
    DatasetDetails,
    DatasetFilters,
    DatasetList,
    DatasetSubmission,
    License,
    Task,
    Visibility,
)
from datacollective.submissions import (
    create_submission_draft,
    create_submission_with_upload,
    submit_submission,
    update_submission,
)
from datacollective.upload import upload_dataset_file, upload_sample_file

__all__ = [
    "download_dataset",
    "save_dataset_to_disk",
    "load_dataset",
    "get_dataset_details",
    "list_datasets",
    "list_dataset_filters",
    "create_submission_draft",
    "update_submission",
    "submit_submission",
    "create_submission_with_upload",
    "upload_dataset_file",
    "upload_sample_file",
    "DatasetDetails",
    "DatasetList",
    "DatasetFilters",
    "DatasetSubmission",
    "License",
    "Task",
    "Visibility",
    "AuthenticationError",
    "RateLimitError",
    "__version__",
]

# DO NOT EDIT THE VERSION MANUALLY, USE bump-my-version TO UPDATE. See release.md
__version__ = "0.6.2"
