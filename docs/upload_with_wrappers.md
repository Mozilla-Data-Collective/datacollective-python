# Uploading with bash wrapper scripts

This guide provides a nicer terminal UX on top of the raw upload API workflow described in [Uploading with curl and the raw API](upload_with_curl.md).

Instead of manually calling each endpoint yourself, you can use the bash scripts in the repository's `scripts/` directory:

- `scripts/mdc-upload-init.sh`
- `scripts/mdc-upload-file.sh`
- `scripts/mdc-upload-update.sh`
- `scripts/mdc-upload-submit.sh`
- `scripts/mdc-upload-workflow.sh`

These scripts keep the upload flow transparent, but smooth out the repetitive parts:

- clearer argument names
- consistent logging to stderr
- JSON parsing with `jq`
- a single metadata JSON file for the update step
- an optional one-command end-to-end workflow

## When to use this approach

Use the bash wrappers if you want:

- a terminal-friendly workflow without writing Python
- a simpler UX than raw `curl`
- copy-pasteable commands that are easier to automate in shell scripts or CI jobs

If you want the most robust upload experience, use the Python SDK guide in [Programmatic Uploads](upload.md). If you want the lowest-level visibility into every request, use [Uploading with curl and the raw API](upload_with_curl.md).

## Prerequisites

Before you begin, make sure you have:

- an MDC API key
- a `.tar.gz` dataset archive
- `bash`, `curl`, and `jq`
- either `sha256sum` or `shasum`
- a dataset archive that is **80 GB or smaller**

Set your environment first:

```bash
export MDC_API_KEY="your-api-key-here"
export MDC_API_URL="https://datacollective.mozillafoundation.org/api"
```

If `MDC_API_URL` is omitted, the scripts default to the production API base URL.

## Metadata file format

The update and workflow commands expect a JSON file containing submission metadata in the same shape as the `DatasetSubmission` API payload.

Example `submission.json`:

```bash
cat > submission.json <<'EOF'
{
  "name": "Dataset Name",
  "shortDescription": "A brief description of the dataset.",
  "longDescription": "A detailed description of the dataset.",
  "locale": "en-US",
  "task": "ASR",
  "format": "TSV",
  "licenseAbbreviation": "CC-BY-4.0",
  "other": "A detailed datasheet-style description of the dataset contents.",
  "restrictions": "No restrictions.",
  "forbiddenUsage": "Do not use for unlawful purposes.",
  "additionalConditions": "No additional conditions.",
  "pointOfContactFullName": "Jane Doe",
  "pointOfContactEmail": "jane@example.com",
  "fundedByFullName": "Funder Name",
  "fundedByEmail": "funder@example.com",
  "legalContactFullName": "Legal Name",
  "legalContactEmail": "legal@example.com",
  "createdByFullName": "Creator Name",
  "createdByEmail": "creator@example.com",
  "intendedUsage": "Describe the intended usage of the dataset.",
  "ethicalReviewProcess": "Describe the ethical review process.",
  "exclusivityOptOut": false
}
EOF
```

The full workflow script requires `name` in this file, because it uses that value when creating the draft submission.

## Quick start: one command for the whole workflow

The fastest way to use the wrappers is the end-to-end script:

```bash
bash scripts/mdc-upload-workflow.sh \
  --file /path/to/dataset.tar.gz \
  --metadata-file submission.json
```

By default, it prints a JSON summary at the end:

```json
{
  "submissionId": "cmxxxxxxxxxxxxxxxxxxxxxxx",
  "fileUploadId": "cmyyyyyyyyyyyyyyyyyyyyyyy",
  "status": "submitted"
}
```

If you prefer simple key/value output instead, use:

```bash
bash scripts/mdc-upload-workflow.sh \
  --file /path/to/dataset.tar.gz \
  --metadata-file submission.json \
  --output summary
```

Which prints:

```text
submissionId=cmxxxxxxxxxxxxxxxxxxxxxxx
fileUploadId=cmyyyyyyyyyyyyyyyyyyyyyyy
status=submitted
```

## Script-by-script usage

### 1. Create a draft submission

```bash
SUBMISSION_ID="$(bash scripts/mdc-upload-init.sh \
  --name "Dataset Name")"

printf 'submissionId=%s\n' "$SUBMISSION_ID"
```

To inspect the full API response instead:

```bash
bash scripts/mdc-upload-init.sh \
  --name "Dataset Name" \
  --output json
```

### 2. Upload the dataset archive

```bash
FILE_UPLOAD_ID="$(bash scripts/mdc-upload-file.sh \
  --file /path/to/dataset.tar.gz \
  --submission-id "$SUBMISSION_ID")"

printf 'fileUploadId=%s\n' "$FILE_UPLOAD_ID"
```

This script handles the multipart upload steps for you:

- create upload session
- request presigned part URLs
- upload parts one by one
- collect `ETag` values
- complete the upload with the part list and SHA-256 checksum

To inspect the upload result payload instead:

```bash
bash scripts/mdc-upload-file.sh \
  --file /path/to/dataset.tar.gz \
  --submission-id "$SUBMISSION_ID" \
  --output json
```

### 3. Update submission metadata

```bash
bash scripts/mdc-upload-update.sh \
  --submission-id "$SUBMISSION_ID" \
  --metadata-file submission.json \
  --file-upload-id "$FILE_UPLOAD_ID"
```

By default, this prints the updated submission JSON. If you only need the submission ID back:

```bash
bash scripts/mdc-upload-update.sh \
  --submission-id "$SUBMISSION_ID" \
  --metadata-file submission.json \
  --file-upload-id "$FILE_UPLOAD_ID" \
  --output id
```

### 4. Submit the draft for review

```bash
STATUS="$(bash scripts/mdc-upload-submit.sh \
  --submission-id "$SUBMISSION_ID")"

printf 'status=%s\n' "$STATUS"
```

The submit wrapper defaults to `--output status`. You can also request the full response:

```bash
bash scripts/mdc-upload-submit.sh \
  --submission-id "$SUBMISSION_ID" \
  --output json
```

## Recommended workflow for local use

For most local shell usage, this is the most practical flow:

```bash
export MDC_API_KEY="your-api-key-here"
export MDC_API_URL="https://datacollective.mozillafoundation.org/api"

cat > submission.json <<'EOF'
{
  "name": "Dataset Name",
  "shortDescription": "A brief description of the dataset.",
  "longDescription": "A detailed description of the dataset.",
  "locale": "en-US",
  "task": "ASR",
  "format": "TSV",
  "licenseAbbreviation": "CC-BY-4.0",
  "other": "A detailed datasheet-style description of the dataset contents.",
  "restrictions": "No restrictions.",
  "forbiddenUsage": "Do not use for unlawful purposes.",
  "additionalConditions": "No additional conditions.",
  "pointOfContactFullName": "Jane Doe",
  "pointOfContactEmail": "jane@example.com",
  "fundedByFullName": "Funder Name",
  "fundedByEmail": "funder@example.com",
  "legalContactFullName": "Legal Name",
  "legalContactEmail": "legal@example.com",
  "createdByFullName": "Creator Name",
  "createdByEmail": "creator@example.com",
  "intendedUsage": "Describe the intended usage of the dataset.",
  "ethicalReviewProcess": "Describe the ethical review process.",
  "exclusivityOptOut": false
}
EOF

bash scripts/mdc-upload-workflow.sh \
  --file /path/to/dataset.tar.gz \
  --metadata-file submission.json \
  --output summary
```

## Error handling and limitations

These wrappers improve ergonomics, but they are still a shell layer over the raw API.

They help with:

- consistent argument parsing
- consistent log messages
- automatic checksum generation
- multipart part upload bookkeeping
- simpler end-to-end execution from one command

They do **not** currently provide the same protections as the Python SDK:

- no automatic resumable upload state file
- no built-in retry loop for transient part upload failures
- no Pydantic validation of metadata before requests are sent
- no progress bar
- no packaged CLI install; the scripts are intended to be run from this repository

If you need those features, prefer the Python SDK workflow in [Programmatic Uploads](upload.md).

## Wrapper scripts vs raw curl vs Python SDK

| Approach | Best for | Trade-offs |
|----------|----------|------------|
| Bash wrappers | Terminal users who want a simpler shell UX | Still less robust than the Python SDK |
| Raw `curl` | Debugging and direct endpoint exploration | Most manual and repetitive |
| Python SDK | Production scripts and the most reliable UX | Requires Python |

## Troubleshooting

### `MDC_API_KEY is required`

Export the API key before running any wrapper:

```bash
export MDC_API_KEY="your-api-key-here"
```

### `Metadata file must contain a non-empty string field: name`

The end-to-end workflow needs `.name` in the metadata file so it can create the draft submission.

### `File exceeds the 80 GB upload limit`

The wrappers perform the same documented size check as the SDK's upload flow.

### Need lower-level visibility?

If you want to see the individual endpoint sequence and payloads, use [Uploading with curl and the raw API](upload_with_curl.md).
