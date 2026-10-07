# Programmatic Uploads

This guide explains how to programmatically upload datasets to the Mozilla Data Collective using the `datacollective` Python SDK.

## Overview

The SDK provides a complete workflow for uploading datasets:

1. **Create a draft submission** - Initialize a new dataset submission
2. **Update submission metadata** - Add required metadata fields to the submission
3. **Upload the dataset file** - Upload your archive using resumable multipart uploads
4. **Upload a sample file** (optional) - Upload a small, representative excerpt of the dataset
5. **Submit for review** - Finalize the submission for review

Additionally, the functionality of uploading a dataset file can be used independently to upload a new archive version to an already approved and published dataset submission. Check the [Upload a New File Version to an Approved Dataset](#upload-a-new-file-version-to-an-approved-dataset) section for more details.

The SDK also supports **resumable uploads**, meaning if an upload is interrupted (network error, system shutdown, etc.), you can resume from where it left off.

## Prerequisites

Before uploading, ensure you have:

- Your Request Access to Upload **approved** in your MDC [profile](https://mozilladatacollective.com/profile/uploads). 
- An API key from the Mozilla Data Collective [dashboard](https://mozilladatacollective.com/api-reference) created **_after_** your request has been approved. Any credentials created before your request was approved will not be able to upload datasets.
- Your dataset packaged as an archive file (`.tar.gz` or `.tgz`, uploads use `application/gzip`)
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
from datacollective import (
    DatasetSubmission,
    License,
    Task,
    Visibility,
    create_submission_with_upload,
)

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
    showContactInfo=False,  # Whether to publicly display the contact information above
    showComplianceAttributes=False,  # Whether to publicly display the dataset's compliance attributes
    visibility=Visibility.PUBLIC,  # public | private | restricted
    isPaid=False,  # True = the dataset is compensated and requires `basePriceCents`,
                   # False (default) = the dataset is free to access
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

To also attach an **optional** sample file, pass `sample_file_path`:

```python
response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    submission=submission,
    sample_file_path="/path/to/dataset-sample.tar.gz",
)
```

See [Uploading a Sample File](#uploading-a-sample-file) for details.

### Visibility

`visibility` controls who can access the dataset and must be one of the `Visibility` enum values: 

| Value                   | Visible to:       | Who can download:                       |
|-------------------------|-------------------|-----------------------------------------|
| `Visibility.PUBLIC`     | Everyone          | Everyone                                |
| `Visibility.RESTRICTED` | Everyone          | Your organization & approved requesters |
| `Visibility.PRIVATE`    | Your organization | Your organization (via SDK)             |

For `Visibility.RESTRICTED` datasets you can also set `autoApproveAccessRequests=True`. The platform
then grants every access request automatically as soon as it is made, instead of leaving it pending
for you to review. The requester's email is shared with you and you can still revoke access later.
The flag defaults to `False` on the platform when left unset. The SDK rejects it for `public`
datasets, which need no access request, and for `private` datasets, which are only visible to your
organization.

```python
submission = DatasetSubmission(
    visibility=Visibility.RESTRICTED,
    autoApproveAccessRequests=True,
)
```


### Pricing

Datasets are free by default (`isPaid` defaults to `False` on the platform when left unset). To
publish a **compensated** dataset, set `isPaid=True` and provide a price in `basePriceCents`:

```python
from datacollective import DatasetSubmission

submission = DatasetSubmission(
    name="Dataset Name",
    # ... other metadata fields ...
    isPaid=True,
    basePriceCents=100_000,  # $1,000.00
)
```

> [!IMPORTANT]
> `basePriceCents` is expressed in **USD cents** (US Dollars), not in dollars.
> For example, `basePriceCents=100_000` sets the price to **$1,000.00 USD**.

> [!NOTE]
> The platform validates that the price falls within an acceptable range and rejects the
> submission otherwise.

- `isPaid=True` requires `basePriceCents` to be set.
- `basePriceCents` cannot be set unless `isPaid=True`, since the price would otherwise be
  ignored and the dataset would stay uncompensated.

To change the price of an existing submission, pass both fields to `update_submission`:

```python
from datacollective import DatasetSubmission, update_submission

update_submission(
    submission_id=submission_id,
    submission=DatasetSubmission(isPaid=True, basePriceCents=250_000),  # $2,500.00
)
```


## Uploading a Sample File

A **sample file** is a small, representative excerpt of your dataset that reviewers and
potential users can inspect without downloading the full archive. It is **optional**: a
submission can be submitted for review without one, and uploading a sample never replaces
the dataset file itself.

Sample files are uploaded exactly like the dataset archive — resumable multipart upload of
a `.tar.gz` archive (`application/gzip`) — through the submission's sample endpoints.

Either pass `sample_file_path` to `create_submission_with_upload`:

```python
from datacollective import create_submission_with_upload

response = create_submission_with_upload(
    file_path="/path/to/dataset.tar.gz",
    submission=submission,
    sample_file_path="/path/to/dataset-sample.tar.gz",
    # Optional, defaults to `<filename>.mdc-sample-upload.json` next to the sample
    sample_state_path="/custom/path/sample-upload-state.json",
)

print(response["submission"]["sampleFileReferenceId"])
```

Or upload it on its own with `upload_sample_file`, using the submission ID (this works for
draft submissions as well as already approved ones):

```python
from datacollective import upload_sample_file

upload_state = upload_sample_file(
    file_path="/path/to/dataset-sample.tar.gz",
    submission_id=submission_id,
)

print(f"Sample upload complete! File Upload ID: {upload_state.fileUploadId}")
```

`upload_sample_file` accepts the same arguments as `upload_dataset_file`
(`state_path`, `show_progress`, `enable_logging`, `part_size`, `max_workers`) and is equally resumable.
Its state file uses a `.mdc-sample-upload.json` suffix, so a sample upload and a dataset
upload never overwrite each other's resume state.

> [!NOTE]
> When using `create_submission_with_upload`, the dataset archive is uploaded first and the
> sample file right after, before the submission is sent for review. A missing
> `sample_file_path` raises `FileNotFoundError` up front, before anything is uploaded.


## Upload a New File Version to an Approved Dataset

Use `upload_dataset_file` when the dataset already exists on the platform and is already in the **Published / Approved** state.

1. Go to **Profile → Uploads** on the platform.
2. Click on the dataset submission you want to upload a new version for (must be in an Approved state).
3. Copy the ID from the URL, for example:
   `https://mozilladatacollective.com/profile/submissions/<ID>`
4. Pass that value to `upload_dataset_file` as `submission_id`.

> [!IMPORTANT]
> The value after `/profile/submissions/` is the **submission ID**, not the dataset ID.

```python
from datacollective import upload_dataset_file

approved_submission_id = "XXXXXXXXXXXXXXXXX"  # submission ID, not dataset ID

upload_state = upload_dataset_file(
    file_path="/path/to/new-dataset-version.tar.gz",
    submission_id=approved_submission_id,
)

print(f"Version upload complete! File Upload ID: {upload_state.fileUploadId}")
```

If the upload is interrupted, rerun the same call and the SDK will resume from the saved state file.

## Required Submission Fields

For a detailed explanation of the required fields in the `DatasetSubmission` model, see the [API Reference](api.md#datacollective.models.DatasetSubmission) section.

To complete the submission process, the submission **must** include at least all of the following fields:

- `name`
- `longDescription`
- `task`
- `locale`
- `format`
- `licenseAbbreviation` or `license`
- `restrictions`
- `forbiddenUsage`
- `pointOfContactFullName`
- `pointOfContactEmail`
- `showContactInfo`
- `visibility`
- `agreeToSubmit=True`

Pricing (`isPaid` and `basePriceCents`) is optional — see [Pricing](#pricing). Datasets are free unless `isPaid=True`.

A completed file upload must also be attached to the submission before it can be submitted for review. The uploaded archive is linked to the submission automatically when the multipart upload completes (the upload is started with the submission's ID). 

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

### Step 2: Update Submission Metadata

Fill in the datasheet fields for your submission. Set the metadata **before**
uploading the file, so that if the upload is later interrupted or fails, the
submission still has its metadata filled in.

```python
from datacollective import (
    DatasetSubmission,
    License,
    Task,
    Visibility,
    update_submission,
)

update_fields = DatasetSubmission(
    task=Task.ASR,
    licenseAbbreviation=License.CC_BY_4_0,
    locale="en-US",
    format="TSV",
    restrictions="No restrictions.",
    forbiddenUsage="Do not use for unlawful purposes.",
    pointOfContactFullName="Jane Doe",
    pointOfContactEmail="jane@example.com",
    showContactInfo=False,
    visibility=Visibility.PUBLIC,
    # ... other metadata fields ...
)

response = update_submission(
    submission_id=submission_id,
    submission=update_fields,
)

print(f"Metadata updated: {response}")
```

### Step 3: Upload the Dataset File

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
> You can also find your submission ID by going to your [Uploads](https://mozilladatacollective.com/profile/uploads) in your profile, click on the dataset submission of your choice, and the URL will contain the submission ID (e.g., `https://mozilladatacollective.com/submissions/cmmjpewijXXXXXXXXX`).

### Step 4 (Optional): Upload a Sample File

```python
from datacollective import upload_sample_file

sample_state = upload_sample_file(
    file_path="/path/to/your/dataset-sample.tar.gz",
    submission_id=submission_id,
)

print(f"Sample upload complete! File Upload ID: {sample_state.fileUploadId}")
```

### Step 5: Submit for Review

```python
from datacollective import DatasetSubmission, submit_submission

response = submit_submission(
    submission_id=submission_id,
    submission=DatasetSubmission(agreeToSubmit=True),
)

submission = response["submission"]
print(f"Submission status: {submission['status']}")
```

## Deleting a Submission

A submission that has not been published can be deleted, together with every file uploaded for it. Use this to discard a draft or a submission still under review. The deletion cannot be undone.

```python
from datacollective import delete_submission

delete_submission(submission_id="your-submission-id")
```

Deleting a published submission, or one that belongs to another organization, raises `PermissionError`.

## Tuning the Part Size

Uploads are **multipart**: the file is split into fixed-size chunks ("parts") that are uploaded concurrently (see [Tuning Upload Concurrency](#tuning-upload-concurrency)). Both `upload_dataset_file` and `create_submission_with_upload` accept a `part_size` argument (in bytes) to control the chunk size. It defaults to **10 MB**.

```python
upload_dataset_file(
    file_path="/path/to/dataset.tar.gz",
    submission_id=submission_id,
    part_size=20 * 1024 * 1024,  # 20 MB parts
)
```

**Why change it?**

- **Larger parts** mean fewer parts overall, so fewer requests and less per-part overhead — useful on fast, reliable connections and for very large files (see the limit below).
- **Smaller parts** give finer-grained resume: after an interruption only the unfinished part is re-uploaded, not a large chunk. Handy on slow or flaky connections. Note that the storage backend requires every part except the last to be at least **5 MB**, so that is the practical lower bound.

**The `MAX_UPLOAD_PARTS` limit**

A single upload can have at most **10,000 parts**. This means `part_size` must be large enough that `ceil(file_size / part_size) ≤ 10,000`. The SDK checks this up front and raises a `ValueError` telling you the minimum `part_size` to use, rather than failing partway through the upload. For example, a 1 TB file needs parts of at least ~100 MB.

> [!NOTE]
> When **resuming** an interrupted upload, `part_size` is ignored — the SDK reuses the part size recorded in the state file so the already-uploaded parts stay valid.

## Tuning Upload Concurrency

Parts are uploaded **in parallel**. `upload_dataset_file`, `upload_sample_file` and `create_submission_with_upload` accept a `max_workers` argument that sets how many parts are being uploaded at once. The default value is **4**.

```python
upload_dataset_file(
    file_path="/path/to/dataset.tar.gz",
    submission_id=submission_id,
    max_workers=8,  # upload 8 parts at a time
)
```

The file is still read and checksummed sequentially on the calling thread. Only the uploads themselves run on worker threads. Parts may finish out of order, but every finished part is recorded in the state file individually, so [resuming](#resumable-uploads) works seamlessly.

### Choosing `max_workers` and `part_size`

`max_workers` controls how many parts are being uploaded at once. `part_size` controls how big each of them is. Together they decide the memory footprint (about `(max_workers + 1) × part_size`), how often the SDK calls the MDC API, and how much work is lost when a part fails.
Finding the right balance of values for these two variables depends on the size of your archive and your internet connection setup.

#### What to increase or decrease and when

| Situation                                        | Suggested change                                                                                                                              |
|--------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| Fast, stable connection and a large archive      | Raise `max_workers` to 8 (or even higher) and keep or raise `part_size`, until throughput stops improving.                                    |
| `RateLimitError` during the upload               | Lower `max_workers`, or raise `part_size` so there are fewer parts and fewer URL requests.                                                    |
| Limited memory on machine (e.g. small cloud VM)  | Lower `max_workers` and/or `part_size` to keep `(max_workers + 1) × part_size` within memory.                                                 |
| Slow or flaky connection, frequent interruptions | Lower `max_workers` if a slow link is being shared by too many streams, and keep `part_size` low to prevent losing too much work on failures. |
| Read timeouts on part uploads                    | Lower `part_size` so each request finishes sooner.                                                                                            |
| Very large file (hundreds of GB or more)         | Raise `part_size` to stay under the 10,000-part limit; then pick `max_workers` from the memory budget.                                        |
| Debugging or reproducing an issue                | Set `max_workers=1` for a sequential, deterministic upload.                                                                                   |

> [!NOTE]
> After an interruption (e.g. Ctrl-C) the call returns immediately, but the process may take a moment to exit while the parts already in flight finish or time out.

#### Debugging

**Too many workers**

- Each worker adds another `part_size` of memory, so a high worker count with large parts can use several GB of RAM.
- More parts start at the same time, which means more near-simultaneous API requests and a higher chance of getting rate limited by our platform (`RateLimitError`).
- Your connection has a fixed bandwidth, and once a few workers are using all of it, adding more does not make the upload faster. The extra workers just split the same bandwidth between them, so each part takes longer while the total time stays the same.

**Too few workers**

- A single connection stream will rarely saturate a fast internet connection link, so the upload takes longer than it needs to. This is most noticeable for large archives on high-bandwidth connections.
- With `max_workers=1` there is no concurrency at all and the upload behaves as a plain sequential upload.

**Too large a part size**

- Fewer parts means fewer API requests and less per-part overhead, but when a part fails or the upload is interrupted the whole part is re-uploaded, so more progress is lost.
- Memory grows with the part size as well, since every in-flight part is held in memory.
- Each part is a single long-running request, so very large parts are more exposed to read timeouts on slow or unstable connections.

**Too small a part size**

- Finer-grained resume: only a small part is re-uploaded after a failure.
- Many more parts means many more API requests, which can trigger the rate limit, and more per-part overhead. The number of parts is also capped at 10,000 (see [Tuning the Part Size](#tuning-the-part-size)), and parts smaller than 5 MB are rejected by storage.


## Resumable Uploads

The SDK automatically handles interrupted uploads using a state file.

### How It Works

1. When an upload starts, the SDK creates a state file (`.mdc-upload.json`) alongside your archive
2. The state file tracks which parts have been successfully uploaded (it is written atomically, so an interruption never leaves it half-written)
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
| `FileNotFoundError` | The specified file path does not exist, or the submission was not found |
| `ResourceRemovedError` | The submission was deleted. Subclasses `FileNotFoundError` |
| `ValidationError` | Invalid `DatasetSubmission` or required string inputs |
| `ValueError` | Missing or invalid required parameter |
| `AuthenticationError` | The API key is invalid, expired or revoked |
| `PermissionError` | Your organization is not approved to upload datasets (or the API key was created before the approval was granted), the submission belongs to another organization, or it has already been published. The error message carries the API's explanation |
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
- [`upload_sample_file`](api.md) - Upload an optional sample file to a submission
- [`submit_submission`](api.md) - Submit a draft for review
- [`delete_submission`](api.md) - Delete an unpublished submission
