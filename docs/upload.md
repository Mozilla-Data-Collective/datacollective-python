# Programmatic Uploads

This guide explains how to programmatically upload datasets to the Mozilla Data Collective using the `datacollective` Python SDK.

## Overview

The SDK provides a complete workflow for uploading datasets:

1. **Create a draft submission** - Initialize a new dataset submission
2. **Upload the dataset file** - Upload your archive using resumable multipart uploads
3. **Submit for review** - Finalize the submission with required metadata

The SDK supports **resumable uploads**, meaning if an upload is interrupted (network error, system shutdown, etc.), you can resume from where it left off.

## Prerequisites

Before uploading, ensure you have:

- An API key from the Mozilla Data Collective [dashboard](https://datacollective.mozillafoundation.org/api-reference)
- Your dataset packaged as an archive file (`.tar.gz`)
- All the required metadata for the dataset submission

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
from datacollective import create_submission_with_upload

# Define all required submission fields
submission_fields = {
    "shortDescription": "A brief description of your dataset",
    "longDescription": "A detailed description of your dataset",
    "locale": "en-US",
    "task": "classification",
    "format": "tar.gz",
    "licenseAbbreviation": "CC-BY",
    "license": "Creative Commons Attribution",
    "licenseUrl": "https://creativecommons.org/licenses/by/4.0/",
    "other": "Additional information about the dataset",
    "restrictions": "Any restrictions on use",
    "forbiddenUsage": "Forbidden use cases",
    "additionalConditions": "Additional conditions",
    "pointOfContactFullName": "Jane Doe",
    "pointOfContactEmail": "jane@example.com",
    "fundedByFullName": "Funder Name",
    "fundedByEmail": "funder@example.com",
    "legalContactFullName": "Legal Contact Name",
    "legalContactEmail": "legal@example.com",
    "createdByFullName": "Creator Name",
    "createdByEmail": "creator@example.com",
    "intendedUsage": "Intended use of the dataset",
    "ethicalReviewProcess": "Description of ethical review",
    "exclusivityOptOut": True,
}

# Upload and submit in one call
response = create_submission_with_upload(
    file_path="/path/to/your/dataset.tar.gz",
    name="My Dataset Name",
    long_description="Full description of my dataset",
    submission_fields=submission_fields,
    mime_type="application/gzip",
)

print(f"Submission ID: {response.get('id')}")
print(f"File Upload ID: {response.get('fileUploadId')}")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | `str` | Yes | Path to the dataset archive on disk |
| `name` | `str` | Yes | Name of the dataset |
| `long_description` | `str` | Yes | Full description of the dataset |
| `submission_fields` | `dict` | Yes | Datasheet fields required for submission (see below) |
| `mime_type` | `str` | Yes | MIME type of the file (e.g., `application/gzip`, `application/zip`) |
| `filename` | `str` | No | Optional filename override for the upload |
| `state_path` | `str` | No | Optional path to persist upload state (defaults to `<filename>.mdc-upload.json`) |
| `resume` | `bool` | No | Whether to resume a previous upload session (default: `True`) |

## Required Submission Fields

When submitting a dataset, you must provide the following fields:

### Dataset Information

| Field | Type | Description |
|-------|------|-------------|
| `shortDescription` | `str` | Brief description of the dataset |
| `longDescription` | `str` | Detailed description of the dataset |
| `locale` | `str` | Language/locale code (e.g., `en-US`, `de-DE`) |
| `task` | `str` | ML task type (e.g., `classification`, `asr`, `tts`) |
| `format` | `str` | File format (e.g., `tar.gz`, `zip`, `parquet`) |

### Licensing

| Field | Type | Description |
|-------|------|-------------|
| `licenseAbbreviation` | `str` | Short license name (e.g., `CC-BY`, `MIT`) |
| `license` | `str` | Full license name |
| `licenseUrl` | `str` | URL to the license text |

### Usage Information

| Field | Type | Description |
|-------|------|-------------|
| `other` | `str` | Additional information about the dataset |
| `restrictions` | `str` | Any restrictions on dataset use |
| `forbiddenUsage` | `str` | Explicitly forbidden use cases |
| `additionalConditions` | `str` | Additional conditions for use |
| `intendedUsage` | `str` | Intended use of the dataset |
| `ethicalReviewProcess` | `str` | Description of ethical review conducted |
| `exclusivityOptOut` | `bool` | Whether to opt out of exclusivity |

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
from datacollective import create_submission_draft

draft = create_submission_draft(
    name="My Dataset Name",
    long_description="Full description of my dataset"
)

submission_id = draft["submissionId"]
print(f"Created draft submission: {submission_id}")
```

### Step 2: Upload the Dataset File

```python
from datacollective import upload_dataset_file

upload_state = upload_dataset_file(
    file_path="/path/to/your/dataset.tar.gz",
    submission_id=submission_id,
    mime_type="application/gzip",
)

print(f"Upload complete! File Upload ID: {upload_state.fileUploadId}")
```

### Step 3: Submit for Review

```python
from datacollective import submit_submission

submission_fields = {
    "shortDescription": "A brief description",
    "longDescription": "Full description",
    # ... all other required fields ...
    "fileUploadId": upload_state.fileUploadId,
}

response = submit_submission(
    submission_id=submission_id,
    submission_fields=submission_fields
)

print(f"Submission submitted for review: {response}")
```

## Resumable Uploads

The SDK automatically handles interrupted uploads using a state file.

### How It Works

1. When an upload starts, the SDK creates a state file (`.mdc-upload.json`) alongside your archive
2. The state file tracks which parts have been successfully uploaded
3. If the upload is interrupted, rerunning the same upload call will resume from where it left off
4. Once the upload completes, the state file is preserved for reference

### Automatic Resume

Simply rerun the same upload call after an interruption:

```python
# First attempt (interrupted)
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    name="My Dataset",
    long_description="Description",
    submission_fields=submission_fields,
    mime_type="application/gzip",
)

# Second attempt (resumes automatically)
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    name="My Dataset",
    long_description="Description",
    submission_fields=submission_fields,
    mime_type="application/gzip",
)
```

### Custom State File Location

You can specify a custom location for the state file:

```python
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    name="My Dataset",
    long_description="Description",
    submission_fields=submission_fields,
    mime_type="application/gzip",
    state_path="/custom/path/upload-state.json",
)
```

### Disabling Resume

To force a fresh upload (ignoring any existing state):

```python
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    name="My Dataset",
    long_description="Description",
    submission_fields=submission_fields,
    mime_type="application/gzip",
    resume=False,
)
```


## Error Handling

The SDK raises specific exceptions for common error cases:

```python
from datacollective import create_submission_with_upload

try:
    response = create_submission_with_upload(
        file_path="/path/to/dataset.tar.gz",
        name="My Dataset",
        long_description="Description",
        submission_fields=submission_fields,
        mime_type="application/gzip",
    )
except FileNotFoundError as e:
    print(f"File not found: {e}")
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
| `ValueError` | Missing or invalid required parameter |
| `PermissionError` | API key is invalid or lacks permissions |
| `RuntimeError` | Rate limit exceeded or upload failed |

## Complete Example

Here's a complete example that uploads a dataset with all required fields:

```python
import os
from datacollective import create_submission_with_upload

# Ensure API key is set
if not os.getenv("MDC_API_KEY"):
    raise EnvironmentError("Please set MDC_API_KEY environment variable")

# Path to your dataset archive
file_path = "/path/to/your/dataset.tar.gz"

# Submission metadata
submission_fields = {
    # Dataset info
    "shortDescription": "Speech recognition dataset for English",
    "longDescription": """
    This dataset contains 10,000 hours of transcribed English speech
    collected from volunteer contributors. The data includes diverse
    accents and speaking styles.
    """,
    "locale": "en-US",
    "task": "asr",
    "format": "tar.gz",
    
    # Licensing
    "licenseAbbreviation": "CC0",
    "license": "Creative Commons Zero",
    "licenseUrl": "https://creativecommons.org/publicdomain/zero/1.0/",
    
    # Usage
    "other": "Includes metadata CSV with speaker demographics",
    "restrictions": "None",
    "forbiddenUsage": "Surveillance applications",
    "additionalConditions": "Attribution appreciated but not required",
    "intendedUsage": "Training and evaluating speech recognition models",
    "ethicalReviewProcess": "IRB approved under protocol #12345",
    "exclusivityOptOut": True,
    
    # Contacts
    "pointOfContactFullName": "Jane Doe",
    "pointOfContactEmail": "jane.doe@example.org",
    "fundedByFullName": "Mozilla Foundation",
    "fundedByEmail": "grants@mozilla.org",
    "legalContactFullName": "John Smith",
    "legalContactEmail": "legal@example.org",
    "createdByFullName": "Jane Doe",
    "createdByEmail": "jane.doe@example.org",
}

# Upload the dataset
response = create_submission_with_upload(
    file_path=file_path,
    name="English Speech Corpus v1.0",
    long_description=submission_fields["longDescription"],
    submission_fields=submission_fields,
    mime_type="application/gzip",
)

print("Upload complete!")
print(f"Submission ID: {response.get('id')}")
print(f"Status: {response.get('status')}")
```

## Using Pydantic Models

For type safety, you can use the provided Pydantic models:

```python
from datacollective import create_submission_with_upload
from datacollective.models import DatasetSubmissionSubmitInput

# Using the Pydantic model for validation
submission_fields = DatasetSubmissionSubmitInput(
    shortDescription="Speech recognition dataset",
    longDescription="Full description...",
    locale="en-US",
    task="asr",
    format="tar.gz",
    licenseAbbreviation="CC0",
    license="Creative Commons Zero",
    licenseUrl="https://creativecommons.org/publicdomain/zero/1.0/",
    other="Additional info",
    restrictions="None",
    forbiddenUsage="None",
    additionalConditions="None",
    pointOfContactFullName="Jane Doe",
    pointOfContactEmail="jane@example.com",
    fundedByFullName="Mozilla Foundation",
    fundedByEmail="mozilla@example.com",
    legalContactFullName="Legal Team",
    legalContactEmail="legal@example.com",
    createdByFullName="Jane Doe",
    createdByEmail="jane@example.com",
    intendedUsage="ASR model training",
    ethicalReviewProcess="IRB approved",
    exclusivityOptOut=True,
)

response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    name="My Dataset",
    long_description="Description",
    submission_fields=submission_fields,
    mime_type="application/gzip",
)
```

## API Reference

For detailed API documentation, see the [API Reference](api.md) section.

### Key Functions

- [`create_submission_with_upload`](api.md) - One-step submission and upload
- [`create_submission_draft`](api.md) - Create a draft submission
- [`upload_dataset_file`](api.md) - Upload a file to a submission
- [`submit_submission`](api.md) - Submit a draft for review
- [`initiate_upload`](api.md) - Start a multipart upload session
- [`get_presigned_part_url`](api.md) - Get a presigned URL for uploading a part
- [`complete_upload`](api.md) - Finalize a multipart upload

### Key Models

- [`DatasetSubmissionSubmitInput`](api.md) - Pydantic model for submission fields
- [`DatasetSubmissionDraftInput`](api.md) - Pydantic model for draft creation
- [`UploadPart`](api.md) - Model representing an uploaded part

