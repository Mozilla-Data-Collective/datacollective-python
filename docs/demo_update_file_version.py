from datacollective import upload_dataset_file

# The dataset must already be approved on the platform.
# Copy the submission ID from:
# https://datacollective.mozillafoundation.org/profile/submissions/<submission-id>
approved_submission_id = "XXXXXXXXXXXXX"

upload_state = upload_dataset_file(
    file_path="example_dataset.tar.gz",
    submission_id=approved_submission_id,
)

print(f"Version upload complete! File Upload ID: {upload_state.fileUploadId}")
