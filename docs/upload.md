# Programmatic Uploads

This guide explains how to programmatically upload datasets to the Mozilla Data Collective using the `datacollective` Python SDK.

If you want a terminal-based workflow without writing Python, see [Uploading with bash wrapper scripts](upload_with_wrappers.md). If you want to call the upload endpoints directly from the terminal instead of using wrappers, see [Uploading with curl and the raw API](upload_with_curl.md).

## Overview

The SDK provides a complete workflow for uploading datasets:

1. **Create a draft submission** - Initialize a new dataset submission
2. **Upload the dataset file** - Upload your archive using resumable multipart uploads
3. **Update submission metadata** - Add required metadata fields to the submission
4. **Submit for review** - Finalize the submission for review

The SDK also supports **resumable uploads**, meaning if an upload is interrupted (network error, system shutdown, etc.), you can resume from where it left off.

## Prerequisites

Before uploading, ensure you have:

- An API key from the Mozilla Data Collective [dashboard](https://datacollective.mozillafoundation.org/api-reference)
- Your dataset packaged as an archive file (`.tar.gz`, uploads use `application/gzip`)
- All the required metadata for the dataset submission
- Dataset archives must be **80GB or less**

### Configuration

Set your API key as an environment variable:

```bash
export MDC_API_KEY=your-api-key-here
```

Or create a `.env` file in your project directory:

```bash
MDC_API_KEY=your-api-key-here
```

## Quick Start: One-Step Upload

The simplest way to upload a dataset is using `create_submission_with_upload`, which handles the entire workflow in a single call:

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

## Required Submission Fields

For a detailed explanation of the required fields in the `DatasetSubmission` model, see the [API Reference](api.md#datacollective.models.DatasetSubmission) section.

Note that to complete the submission process, you must set `agreeToSubmit=True` in the `DatasetSubmission` model, which confirms that you have the right to submit the dataset and that all information provided is accurate.

## Step-by-Step Upload

For more control over the upload process, you can use the individual functions:

### Step 1: Create a Draft Submission

```python
from datacollective import DatasetSubmission, create_submission_draft

submission = DatasetSubmission(
    name="Dataset Name",
    longDescription="A detailed description of the dataset.",
)

draft = create_submission_draft(submission)

submission_id = draft["submission"]["id"]
print(f"Created draft submission: {submission_id}")
```

Which should output something like:

```
Created draft submission: cmmjpewijXXXXXXXXX
```

### Step 2: Upload the Dataset File

Then, you can use the submission ID above to upload the dataset file:

```python
from datacollective import upload_dataset_file

upload_state = upload_dataset_file(
    file_path="/path/to/your/dataset.tar.gz",
    submission_id=submission_id,
)

print(f"Upload complete! File Upload ID: {upload_state.fileUploadId}")
```

> [!TIP]
> You can also find your submission ID by going to your [Uploads](https://datacollective.mozillafoundation.org/profile/uploads) in your profile, click on the dataset submission of your choice, and the URL will contain the submission ID (e.g., `https://datacollective.mozillafoundation.org/submissions/cmmjpewijXXXXXXXXX`).

### Step 3: Update Submission Metadata

For this step, you will need the `fileUploadId` from the upload response above, which is required to link the uploaded file to your submission. Without this ID, you won't be able to proceed to the submission step. If you no longer have access to it, you will need to re-upload the file to get a new `fileUploadId`.

At this step, you can also update any other metadata fields.

```python
from datacollective import DatasetSubmission, License, Task, update_submission

update_fields = DatasetSubmission(
    task=Task.ASR,
    licenseAbbreviation=License.CC_BY_4_0,
    locale="en-US",
    format="TSV",
    restrictions="No restrictions.",
    forbiddenUsage="Do not use for unlawful purposes.",
    pointOfContactFullName="Jane Doe",
    pointOfContactEmail="jane@example.com",
    fileUploadId=upload_state.fileUploadId,
    # ... other metadata fields ...
)

response = update_submission(
    submission_id=submission_id,
    submission=update_fields,
)

print(f"Metadata updated: {response}")
```

### Step 4: Submit for Review

```python
from datacollective import DatasetSubmission, submit_submission

response = submit_submission(
    submission_id=submission_id,
    submission=DatasetSubmission(agreeToSubmit=True),
)

submission = response["submission"]
print(f"Submission status: {submission['status']}")
```

## Resumable Uploads

The SDK automatically handles interrupted uploads using a state file.

### How It Works

1. When an upload starts, the SDK creates a state file (`.mdc-upload.json`) alongside your archive
2. The state file tracks which parts have been successfully uploaded
3. If the upload is interrupted, rerunning the same upload call will resume from where it left off
4. Once the upload completes successfully, the state file is removed automatically

### Automatic Resume

Simply rerun the same upload call after an interruption.

#### Using create_submission_with_upload

```python

from datacollective import create_submission_with_upload

# First attempt (interrupted)
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    submission=submission
)
# Second attempt (resumes automatically)
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    submission=submission
)
```

#### Using upload_dataset_file

```python
from datacollective import upload_dataset_file

# First attempt (interrupted)
upload_state = upload_dataset_file(
    file_path="/path/to/your/dataset.tar.gz",
    submission_id=submission_id,
)

# Second attempt (resumes automatically)
upload_state = upload_dataset_file(
    file_path="/path/to/your/dataset.tar.gz",
    submission_id=submission_id,
)
```

### Custom State File Location

You can specify a custom location for the state file:

```python
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    submission=submission,
    state_path="/custom/path/upload-state.json",
)
```

### Disabling Resume

To force a fresh upload (ignoring any existing state), simply delete the state file 
(<filename>.mdc-upload.json) created by the SDK before starting another upload.

## Error Handling

The SDK raises specific exceptions for common error cases:

| Exception | Cause |
|-----------|-------|
| `FileNotFoundError` | The specified file path does not exist |
| `ValidationError` | Invalid `DatasetSubmission` or required string inputs |
| `ValueError` | Missing or invalid required parameter |
| `PermissionError` | API key is invalid or lacks permissions |
| `RuntimeError` | Rate limit exceeded or upload failed |

## Using the DatasetSubmission Model

All submission inputs use the `DatasetSubmission` Pydantic model, so validation happens
as soon as you construct the model (before any network calls are made).

## API Reference

For detailed API documentation, see the [API Reference](api.md) section.

### Key Functions

- [`create_submission_with_upload`](api.md) - One-step submission and upload
- [`create_submission_draft`](api.md) - Create a draft submission
- [`update_submission`](api.md) - Update submission metadata
- [`upload_dataset_file`](api.md) - Upload a file to a submission
- [`submit_submission`](api.md) - Submit a draft for review
