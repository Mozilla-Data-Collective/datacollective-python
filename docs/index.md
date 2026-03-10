# Mozilla Data Collective Python SDK Library

Welcome to the documentation for the `datacollective` Python client for the
[Mozilla Data Collective](https://datacollective.mozillafoundation.org/) REST API.

This library helps you:

- Authenticate with the Mozilla Data Collective.
- Download datasets to local storage.
- Load supported datasets into AI-friendly formats, such as pandas DataFrames.

## Installation

Install from PyPI:

```bash
pip install datacollective
```


## Getting an API Key

To use the Mozilla Data Collective API, you need an API key:

1. Sign up to the [Mozilla Data Collective](https://datacollective.mozillafoundation.org/) platform.
2. Create or retrieve an API key from your Account -> Credentials page.
3. Store your key secret in a `.env` file and do not commit it to version control (git).

## Configuration

The client reads configuration from environment variables and `.env` files.

### Environment variables

Required:

- `MDC_API_KEY` - Your Mozilla Data Collective API key.

Optional:

- `MDC_API_URL` - API endpoint (defaults to the production URL).
- `MDC_DOWNLOAD_PATH` - Local directory where datasets will be downloaded
  (defaults to `~/.mozdata/datasets`).

Example using environment variables directly:

```bash
export MDC_API_KEY=your-api-key-here
export MDC_API_URL=https://datacollective.mozillafoundation.org/api
export MDC_DOWNLOAD_PATH=~/.mozdata/datasets
```

### `.env` file

The client will automatically load configuration from a `.env` file in your
project root or present working directory.

Create a file named `.env`:

```bash
# MDC API Configuration
MDC_API_KEY=your-api-key-here
MDC_API_URL=https://datacollective.mozillafoundation.org/api
MDC_DOWNLOAD_PATH=~/.mozdata/datasets
```

!!! warning "Security note"
    Do not commit `.env` files to version control, as they contain secrets.

## Basic Usage


**IMPORTANT NOTE:** Before trying to access any dataset, make sure you have thoroughly **read and agreed** to the specific dataset's conditions & licensing terms.

!!! tip
    You can find the `dataset-id` by looking at the URL of the dataset's page on MDC platform. The ID is the unique string of characters located at the very end of the URL, after the `/datasets/` path. For example, for URL `https://datacollective.mozillafoundation.org/datasets/cmflnuzw6lrt9e6ui4kwcshvn` dataset id will be `cmflnuzw6lrt9e6ui4kwcshvn`.

### Download a dataset

Use `save_dataset_to_disk` to download a dataset to the configured download path:

```python
from datacollective import save_dataset_to_disk

dataset = save_dataset_to_disk("your-dataset-id")

# Depending on the implementation, `dataset` may contain metadata
# about the downloaded files or a higher-level dataset object.
```

The files will be stored under `MDC_DOWNLOAD_PATH` (default `~/.mozdata/datasets`).

## Programmatic submissions and uploads

The SDK supports creating dataset submissions and uploading files with resumable uploads. 
The upload state is stored in a JSON file alongside the archive so interrupted uploads can resume automatically.

```python
from datacollective import DatasetSubmission, License, Task, create_submission_with_upload

submission = DatasetSubmission(
    name="Dataset Name",
    longDescription="A detailed description of the dataset.",
    shortDescription="A brief description of the dataset.",
    locale="en-US",
    task=Task.ASR,
    format="TSV",
    licenseAbbreviation=License.CC_BY_4_0,
    other="This text should provide a detailed description of the dataset, "
          "including its contents, structure, and any relevant information "
          "that would help users understand what the dataset is about "
          "and how it can be used.",
    restrictions="Any restrictions you want to impose on the dataset",
    forbiddenUsage="Use cases that are not allowed with this dataset",
    additionalConditions="Any additional conditions for using the dataset",
    pointOfContactFullName="Jane Doe",
    pointOfContactEmail="jane@example.com",
    fundedByFullName="Funder Name",
    fundedByEmail="funder@example.com",
    legalContactFullName="Legal Name",
    legalContactEmail="legal@example.com",
    createdByFullName="Creator Name",
    createdByEmail="creator@example.com",
    intendedUsage="Describe the intended usage of the dataset, including "
                  "potential applications and use cases.",
    ethicalReviewProcess="Describe the ethical review process that was "
                         "followed for this dataset, including any approvals "
                         "or considerations related to data collection and usage.",
    exclusivityOptOut=False,  # True = This dataset is non-exclusive to Mozilla Data Collective, 
                              # False = Dataset is exclusively hosted in Mozilla Data Collective
    agreeToSubmit=True,  # True = You confirm that you have the right to submit this dataset and 
                         # that all information provided in the datasheet is accurate. 
                         # Required to be True to complete the submission process
)

response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    submission=submission
)

print(response)
```

For predefined licenses, pass `licenseAbbreviation=License.<VALUE>` and leave `licenseUrl` and `license` unset. For a custom license, pass a custom string to `license` and optionally include `licenseUrl` and `licenseAbbreviation`.

> [!TIP]
> If a file upload is interrupted, simply rerun the same function above and the upload will resume from where it left off.

## Loading and Querying Datasets

!!! note
    In-memory dataset loading is currently supported only for certain datasets.

You can load supported datasets into memory as a `pandas` `DataFrame` for
analysis:

```python
from datacollective import load_dataset

df = load_dataset("your-dataset-id")

# Inspect the loaded DataFrame
print(df.head())
```

Once loaded into a `DataFrame`, you can use standard `pandas` operations
to filter, aggregate, and analyze the data.

> For details on how `schema.yaml` files drive the loading process, see
> [Schema-Based Dataset Loading](schema_parse.md).

## Get dataset details

You can retrieve info from the datasheet of a dataset without downloading it:

```python
from datacollective import get_dataset_details

info = get_dataset_details("your-dataset-id")
print(info)
```

### Automatic Download Resume

The SDK automatically handles interrupted downloads. If a download is interrupted
for any reason (network error, user cancellation, system shutdown, etc.), the SDK
will automatically resume from where it left off when you call `save_dataset_to_disk`
or `load_dataset` again.

**How it works:**

1. When a download starts, the SDK creates a `.checksum` file alongside the partial
   download (`.part` file) to track the download state.
2. If the download is interrupted, both files are preserved.
3. On the next download attempt, the SDK detects the partial download and resumes
   from the last byte received.
4. Once the download completes successfully, the temporary files are automatically
   cleaned up.

!!! tip
    You don't need to do anything special to enable resume functionality, it works
    automatically. Just call the same function again after an interruption.

**Edge cases handled:**

- If the dataset has been updated since the interrupted download, the SDK detects
  the checksum mismatch and starts a fresh download.
- If only partial files exist without proper tracking data, the SDK safely starts
  a fresh download.


### Automatically check for extracted archives

The `load_dataset` function avoids redundant extraction by automatically detecting existing files. 
It checks if the dataset archive is already downloaded and the folder is extracted under the same name. 
This behavior applies when `overwrite_existing=False` and `overwrite_extracted=False`. 
The SDK identifies the data if the extracted folder name matches the archive name without the extension.

## API Reference

For a detailed API reference, see the [API Reference](api.md) section of the documentation.

!!! note
    This section is intended for maintainers of the `datacollective` library.

## Tests

Run the full test suite:
```bash
pytest -v
```

Note that the e2e tests require a valid `MDC_TEST_API_KEY` and a `MDC_TEST_API_URL` key set in your environment. Pytest will skip the live e2e tests automatically if either is missing.


## Release Workflow

Check out the [Release Workflow](release.md) document for details on how to
publish new versions of the library to PyPI using GitHub Actions.
