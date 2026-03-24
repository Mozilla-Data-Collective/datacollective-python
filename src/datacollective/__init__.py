"""Mozilla Data Collective Python Client Library."""

from datacollective.datasets import (
    download_dataset,
    get_dataset_details,
    load_dataset,
    save_dataset_to_disk,
)
from datacollective.models import DatasetSubmission, License, Task
from datacollective.submissions import (
    create_submission_draft,
    create_submission_with_upload,
    submit_submission,
    update_submission,
)
from datacollective.upload import upload_dataset_file

__all__ = [
    "download_dataset",
    "save_dataset_to_disk",
    "load_dataset",
    "get_dataset_details",
    "create_submission_draft",
    "update_submission",
    "submit_submission",
    "create_submission_with_upload",
    "upload_dataset_file",
    "DatasetSubmission",
    "License",
    "Task",
    "__version__",
]

# DO NOT EDIT THE VERSION MANUALLY, USE bump-my-version TO UPDATE. See release.md
__version__ = "0.4.4"
