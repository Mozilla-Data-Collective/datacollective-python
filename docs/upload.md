# Programmatic Uploads

This guide explains how to programmatically upload datasets to the Mozilla Data Collective using the `datacollective` Python SDK.

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
from datacollective import DatasetSubmission, Task, create_submission_with_upload

submission = DatasetSubmission(
    name="Dataset Name",
    longDescription="A detailed description of the dataset.",
    shortDescription="A brief description of the dataset.",
    locale="en-US",
    task=Task.ASR,
    format="TSV",
    licenseAbbreviation="CC-BY-4.0",
    license="Creative Commons Attribution",
    licenseUrl="https://creativecommons.org/licenses/by/4.0/",
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

### Parameters

| Parameter | Type                | Required | Description                                                                                                                                                                        |
|-----------|---------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `file_path` | `str`               | Yes | Path to the dataset archive on disk                                                                                                                                                |
| `submission` | `DatasetSubmission` | Yes | Submission metadata model                                                                                                                                                          |
| `agreeToSubmit` | `bool`              | Yes | You confirm that you have the right to submit this dataset and that all information provided in the datasheet is accurate.  Required to be True to complete the submission process |
| `submission_id` | `str`               | No | Existing submission ID to resume instead of creating a new draft                                                                                                                   |
| `state_path` | `str`               | No | Optional path to persist upload state file (defaults to `<filename>.mdc-upload.json`)                                                                                              |
| `resume` | `bool`              | No | Whether to resume a previous upload session (default: `True`)                                                                                                                      |

## Required Submission Fields

When submitting a dataset, you must provide the following fields:

### Dataset Information

| Field | Type | Description                                                                                                                                                        |
|-------|------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `shortDescription` | `str` | Brief description of the dataset                                                                                                                                   |
| `longDescription` | `str` | Detailed description of the dataset                                                                                                                                |
| `locale` | `str` | Language/locale code (e.g., `en-US`, `de-DE`)                                                                                                                      |
| `task` | `Task` | ML task type — must be one of the [`Task`](api.md) enum values: `N/A`, `NLP`, `ASR`, `LI`, `TTS`, `MT`, `LM`, `LLM`, `NLU`, `NLG`, `CALL`, `RAG`, `CV`, `ML`, `Other` |
| `format` | `str` | File format (e.g., `TSV`, `WAV`)                                                                                                                           |

### Licensing

| Field | Type | Description                                   |
|-------|------|-----------------------------------------------|
| `licenseAbbreviation` | `str` | Short license name (e.g., `CC-BY-4.0`, `MIT`) |
| `license` | `str` | Full license name                             |
| `licenseUrl` | `str` | URL to the license text                       |

### Usage Information

| Field                  | Type | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|------------------------|------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `other`                | `str` | The datasheet of the dataset                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `restrictions`         | `str` | Any restrictions on dataset use                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `forbiddenUsage`       | `str` | Explicitly forbidden use cases                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `additionalConditions` | `str` | Additional conditions for use                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `intendedUsage`        | `str` | Intended use of the dataset                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `ethicalReviewProcess` | `str` | Description of ethical review conducted                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `exclusivityOptOut`    | `bool` | True = This dataset is non-exclusive to Mozilla Data Collective, False = Dataset is exclusively hosted in Mozilla Data Collective. Mozilla Data Collective provides protections, management controls and visibility for Datasets hosted on the Platform. These safeguards and insights apply in full when your Dataset is hosted exclusively on the Platform. If your Dataset will also be hosted or made accessible in other places, certain of these protections and visibility features may not apply. Check this box if Mozilla Data Collective will not be the exclusive hosting and point for your Dataset. See more details [here](https://datacollective.mozillafoundation.org/terms/providers#appendix-1). |

### Contact Information

| Field | Type | Description |
|-------|------|-------------|
| `pointOfContactFullName` | `str` | Primary contact name |
| `pointOfContactEmail` | `str` | Primary contact email |
| `fundedByFullName` | `str` | Funder's name |
| `fundedByEmail` | `str` | Funder's email |
| `legalContactFullName` | `str` | Legal contact name |
| `legalContactEmail` | `str` | Legal contact email |
| `createdByFullName` | `str` | Creator's name |
| `createdByEmail` | `str` | Creator's email |

## Step-by-Step Upload

For more control over the upload process, you can use the individual functions:

### Step 1: Create a Draft Submission

```python
from datacollective import DatasetSubmission, create_submission_draft

submission = DatasetSubmission(
    name="My Dataset Name",
    longDescription="Full description of my dataset",
)

draft = create_submission_draft(submission)

submission_id = draft["submission"]["id"]
print(f"Created draft submission: {submission_id}")
```

### Step 2: Upload the Dataset File

```python
from datacollective import upload_dataset_file

upload_state = upload_dataset_file(
    file_path="/path/to/your/dataset.tar.gz",
    submission_id=submission_id,
)

print(f"Upload complete! File Upload ID: {upload_state.fileUploadId}")
```

### Step 3: Update Submission Metadata

```python
from datacollective import DatasetSubmission, Task, update_submission

update_fields = DatasetSubmission(
    task=Task.ML,
    licenseAbbreviation="CC-BY-4.0",
    locale="en-US",
    format="text",
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

Simply rerun the same upload call after an interruption:

```python
submission = DatasetSubmission(
    name="My Dataset",
    longDescription="Description",
    agreeToSubmit=True,
)

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

### Resume an Existing Submission

If you already created a draft submission (or a previous run created one before failing),
pass its ID to resume the workflow without creating a new draft:

```python
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    submission=submission,
    submission_id="existing-submission-id",
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

To force a fresh upload (ignoring any existing state):

```python
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    submission=submission,
    resume=False,
)
```

## Error Handling

The SDK raises specific exceptions for common error cases:

```python
from datacollective import DatasetSubmission, create_submission_with_upload

try:
    response = create_submission_with_upload(
        file_path="/path/to/dataset.tar.gz",
        submission=DatasetSubmission(
            name="My Dataset",
            longDescription="Description",
            agreeToSubmit=True,
        ),
    )
except FileNotFoundError as e:
    print(f"File not found: {e}")
except ValidationError as e:
    print(f"Validation error: {e}")
except ValueError as e:
    print(f"Invalid input: {e}")
except PermissionError as e:
    print(f"Access denied: {e}")
except RuntimeError as e:
    print(f"Upload error: {e}")
```

### Common Errors

| Exception | Cause |
|-----------|-------|
| `FileNotFoundError` | The specified file path does not exist |
| `ValidationError` | Invalid `DatasetSubmission` or required string inputs |
| `ValueError` | Missing or invalid required parameter |
| `PermissionError` | API key is invalid or lacks permissions |
| `RuntimeError` | Rate limit exceeded or upload failed |

## Complete Example

Here's a complete example that uploads a dataset with all required fields:

```python
import os
from datacollective import DatasetSubmission, Task, create_submission_with_upload

# Ensure API key is set
if not os.getenv("MDC_API_KEY"):
    raise EnvironmentError("Please set MDC_API_KEY environment variable")

# Path to your dataset archive
file_path = "/path/to/your/dataset.tar.gz"

# Submission metadata
submission = DatasetSubmission(
    # Dataset info
    name="English Speech Corpus v1.0",
    shortDescription="Speech recognition dataset for English",
    longDescription="""
    This dataset contains 10,000 hours of transcribed English speech
    collected from volunteer contributors. The data includes diverse
    accents and speaking styles.
    """,
    locale="en-US",
    task=Task.ASR,
    format="tar.gz",

    # Licensing
    licenseAbbreviation="CC0",
    license="Creative Commons Zero",
    licenseUrl="https://creativecommons.org/publicdomain/zero/1.0/",

    # Usage
    other="Includes metadata CSV with speaker demographics",
    restrictions="None",
    forbiddenUsage="Surveillance applications",
    additionalConditions="Attribution appreciated but not required",
    intendedUsage="Training and evaluating speech recognition models",
    ethicalReviewProcess="IRB approved under protocol #12345",
    exclusivityOptOut=True,

    # Contacts
    pointOfContactFullName="Jane Doe",
    pointOfContactEmail="jane.doe@example.org",
    fundedByFullName="Mozilla Foundation",
    fundedByEmail="grants@mozilla.org",
    legalContactFullName="John Smith",
    legalContactEmail="legal@example.org",
    createdByFullName="Jane Doe",
    createdByEmail="jane.doe@example.org",

    # Submission
    agreeToSubmit=True,
)

# Upload the dataset
response = create_submission_with_upload(
    file_path=file_path,
    submission=submission
)

print("Upload complete!")
submission_response = response.get("submission", {})
print(f"Submission ID: {submission_response.get('id')}")
print(f"Status: {submission_response.get('status')}")
```

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

### Key Models

- [`DatasetSubmission`](api.md) - Pydantic model for submission metadata
- [`UploadPart`](api.md) - Model representing an uploaded part
