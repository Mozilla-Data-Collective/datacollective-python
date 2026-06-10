import json
from datacollective import get_dataset_details, download_dataset, load_dataset

dataset_id_or_slug = "cmkfm9fbl00nto0070sdcrak2"
download_directory = "downloads"

details = get_dataset_details(dataset_id=dataset_id_or_slug)

print(
    f"Will download dataset with the following details: {json.dumps(details, indent=4)}"
)

path = download_dataset(
    dataset_id=dataset_id_or_slug,
    download_directory=download_directory,
    show_progress=True,
    overwrite_existing=False,
    enable_logging=True,
)

print(f"Will load dataset as DataFrame from local directory: {path}")

df = load_dataset(
    dataset_id=dataset_id_or_slug,
    download_directory=download_directory,
    show_progress=True,
    overwrite_existing=False,
    overwrite_extracted=False,
    enable_logging=True,
)

print(df.head())
