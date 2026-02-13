"""
Mozilla Data Collective Python Client Library
"""

from .datasets import get_dataset_details, load_dataset, save_dataset_to_disk
from .models import (
    DatasetSubmission,
    DatasetSubmissionDraftInput,
    DatasetSubmissionSubmitInput,
)
from .submissions import (
    create_submission_draft,
    create_submission_with_upload,
    submit_submission,
)
from .uploads import (
    complete_upload,
    get_presigned_part_url,
    initiate_upload,
    upload_dataset_file,
)

__all__ = [
    "save_dataset_to_disk",
    "load_dataset",
    "get_dataset_details",
    "create_submission_draft",
    "submit_submission",
    "create_submission_with_upload",
    "initiate_upload",
    "get_presigned_part_url",
    "complete_upload",
    "upload_dataset_file",
    "DatasetSubmission",
    "DatasetSubmissionDraftInput",
    "DatasetSubmissionSubmitInput",
    "__version__",
]

# DO NOT EDIT THE VERSION MANUALLY, USE bump-my-version TO UPDATE. See release.md
__version__ = "0.3.0"
