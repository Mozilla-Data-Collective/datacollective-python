from datacollective import upload_dataset_file

# The dataset must already be approved on the platform.
# Copy the dataset ID or slug from:
# https://datacollective.mozillafoundation.org/datasets/<dataset-id-or-slug>
dataset_id_or_slug = "XXXXXXXXXXXXX"

upload_state = upload_dataset_file(
    file_path="example_dataset.tar.gz",
    dataset_id_or_slug=dataset_id_or_slug,
)

print(f"Version upload complete! File Upload ID: {upload_state.fileUploadId}")
