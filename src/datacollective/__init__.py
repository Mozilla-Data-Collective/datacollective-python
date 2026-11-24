"""Mozilla Data Collective Python Client Library."""

from datacollective.datasets import (
    download_dataset,
    download_sample,
    get_dataset_details,
    list_dataset_filters,
    list_datasets,
    load_dataset,
    save_dataset_to_disk,
)
from datacollective.errors import (
    AuthenticationError,
    RateLimitError,
    ResourceRemovedError,
)
from datacollective.models import (
    CommercialAvailability,
    CommercialAvailabilityCondition,
    ComplianceAnswer,
    ComplianceAttributes,
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
    delete_submission,
    submit_submission,
    update_submission,
)
from datacollective.upload import upload_dataset_file, upload_sample_file

__all__ = [
    "AuthenticationError",
    "CommercialAvailability",
    "CommercialAvailabilityCondition",
    "ComplianceAnswer",
    "ComplianceAttributes",
    "DatasetDetails",
    "DatasetFilters",
    "DatasetList",
    "DatasetSubmission",
    "License",
    "RateLimitError",
    "ResourceRemovedError",
    "Task",
    "Visibility",
    "__version__",
    "create_submission_draft",
    "create_submission_with_upload",
    "delete_submission",
    "download_dataset",
    "download_sample",
    "get_dataset_details",
    "list_dataset_filters",
    "list_datasets",
    "load_dataset",
    "save_dataset_to_disk",
    "submit_submission",
    "update_submission",
    "upload_dataset_file",
    "upload_sample_file",
]

# DO NOT EDIT THE VERSION MANUALLY, USE bump-my-version TO UPDATE. See release.md
__version__ = "0.7.1"
