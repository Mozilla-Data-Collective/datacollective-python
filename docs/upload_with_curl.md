# Uploading with curl and the raw API

This guide shows how to perform the same dataset upload workflow as the Python SDK, but by calling the Mozilla Data Collective API directly with `curl`.

If you want a simpler shell-based UX while still staying outside Python, see [Uploading with bash wrapper scripts](upload_with_wrappers.md).

It covers the practical happy path for:

1. **Create a draft submission**
2. **Upload the dataset archive** with the raw multipart upload endpoints
3. **Update submission metadata** and attach the uploaded file
4. **Submit the dataset for review**

This workflow is useful if you want to script uploads from the terminal, integrate with another language, or debug API behavior directly.

**NOTE:** This workflow is experimental and does not include all the features of the Python SDK, such as automatic retries, resumable uploads, and input validation. For a more robust and convenient upload experience, we recommend using the Python SDK as described in [Programmatic Uploads](upload.md).

## Overview

The raw API upload flow is:

1. `POST /submissions` - create a draft submission
2. `POST /uploads` - initiate a multipart upload session
3. `GET /uploads/{fileUploadId}/parts/{partNumber}` - request a presigned URL for each part
4. `PUT` each part to the returned presigned URL
5. `POST /uploads/{fileUploadId}` - complete the multipart upload with the uploaded part list and file checksum
6. `PATCH /submissions/{submissionId}` - attach `fileUploadId` and update metadata
7. `POST /submissions/{submissionId}` - submit the dataset for review

## Prerequisites

Before you begin, make sure you have:

- An API key from the Mozilla Data Collective [dashboard](https://datacollective.mozillafoundation.org/api-reference)
- Your dataset packaged as a `.tar.gz` archive
- A terminal with `curl` and `jq`
- A dataset archive that is **80 GB or smaller**
- All required dataset metadata ready for the final submission step

### Configuration

Set your API key in the shell:

```bash
export MDC_API_KEY="your-api-key-here"
export MDC_API_URL="https://datacollective.mozillafoundation.org/api"
```

If you omit `MDC_API_URL`, the examples below assume the production API URL shown above.

Set a couple of helper variables:

```bash
export API_BASE="${MDC_API_URL:-https://datacollective.mozillafoundation.org/api}"
export FILE_PATH="/path/to/dataset.tar.gz"
export FILE_NAME="$(basename "$FILE_PATH")"
```

## Required tools

Check that `curl` and `jq` are available:

```bash
command -v curl
command -v jq
```

For the checksum step later, Linux typically provides `sha256sum`, while macOS typically provides `shasum`.

## Step 1: Create a draft submission

Create a new draft submission. The draft creation request only needs a dataset name.

```bash
DRAFT_RESPONSE=$(curl --silent --show-error --fail \
  --request POST \
  --url "$API_BASE/submissions" \
  --header "Authorization: Bearer $MDC_API_KEY" \
  --header 'Content-Type: application/json' \
  --data "$(jq -n \
    --arg name 'Dataset Name' \
    '{name: $name}')")

printf '%s\n' "$DRAFT_RESPONSE" | jq

export SUBMISSION_ID="$(printf '%s' "$DRAFT_RESPONSE" | jq -r '.submission.id')"
echo "Created draft submission: $SUBMISSION_ID"
```

You should get back a response containing a `submission.id` value.

## Step 2: Upload the dataset archive

The upload flow uses several API calls under the hood:

1. Create an upload session with `POST /uploads`
2. Request one presigned URL per part with `GET /uploads/{fileUploadId}/parts/{partNumber}`
3. Upload each part directly to the presigned storage URL
4. Complete the upload with `POST /uploads/{fileUploadId}`

### Upload script

The shell script below performs the full multipart upload and prints the resulting `fileUploadId`.

It:

- asks the API for the multipart upload session
- calculates the file checksum
- uploads each part in order
- records each returned `ETag`
- completes the upload

```bash
cat > upload_mdc_file.sh <<'EOF'
#!/usr/bin/env sh
set -eu

if [ -z "${MDC_API_KEY:-}" ]; then
  echo "MDC_API_KEY is required" >&2
  exit 1
fi

API_BASE="${MDC_API_URL:-https://datacollective.mozillafoundation.org/api}"
FILE_PATH="${1:?usage: upload_mdc_file.sh /path/to/dataset.tar.gz submission-id}"
SUBMISSION_ID="${2:?usage: upload_mdc_file.sh /path/to/dataset.tar.gz submission-id}"

if [ ! -f "$FILE_PATH" ]; then
  echo "File not found: $FILE_PATH" >&2
  exit 1
fi

FILE_NAME="$(basename "$FILE_PATH")"
FILE_SIZE="$(wc -c < "$FILE_PATH" | tr -d ' ')"

if [ "$FILE_SIZE" -le 0 ]; then
  echo "File must be non-empty: $FILE_PATH" >&2
  exit 1
fi

if [ "$FILE_SIZE" -gt 80000000000 ]; then
  echo "File exceeds the 80 GB upload limit: $FILE_PATH" >&2
  exit 1
fi

log() {
  printf '%s\n' "$*" >&2
}

compute_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

CHECKSUM="$(compute_sha256 "$FILE_PATH")"
log "Checksum: $CHECKSUM"

INITIATE_RESPONSE=$(curl --silent --show-error --fail \
  --request POST \
  --url "$API_BASE/uploads" \
  --header "Authorization: Bearer $MDC_API_KEY" \
  --header 'Content-Type: application/json' \
  --data "$(jq -n \
    --arg submissionId "$SUBMISSION_ID" \
    --arg filename "$FILE_NAME" \
    --arg mimeType 'application/gzip' \
    --argjson fileSize "$FILE_SIZE" \
    '{submissionId: $submissionId, filename: $filename, fileSize: $fileSize, mimeType: $mimeType}')")

FILE_UPLOAD_ID="$(printf '%s' "$INITIATE_RESPONSE" | jq -r '.fileUploadId')"
UPLOAD_ID="$(printf '%s' "$INITIATE_RESPONSE" | jq -r '.uploadId')"
PART_SIZE="$(printf '%s' "$INITIATE_RESPONSE" | jq -r '.partSize')"

if [ -z "$FILE_UPLOAD_ID" ] || [ "$FILE_UPLOAD_ID" = "null" ]; then
  echo "Upload initiation did not return fileUploadId" >&2
  exit 1
fi

if [ -z "$PART_SIZE" ] || [ "$PART_SIZE" = "null" ]; then
  echo "Upload initiation did not return partSize" >&2
  exit 1
fi

TOTAL_PARTS=$(( (FILE_SIZE + PART_SIZE - 1) / PART_SIZE ))
PARTS_JSON='[]'

log "Upload session created"
log "fileUploadId=$FILE_UPLOAD_ID"
log "uploadId=$UPLOAD_ID"
log "partSize=$PART_SIZE"
log "totalParts=$TOTAL_PARTS"

PART_NUMBER=1
while [ "$PART_NUMBER" -le "$TOTAL_PARTS" ]; do
  log "Uploading part $PART_NUMBER/$TOTAL_PARTS"

  PRESIGNED_RESPONSE=$(curl --silent --show-error --fail \
    --request GET \
    --url "$API_BASE/uploads/$FILE_UPLOAD_ID/parts/$PART_NUMBER" \
    --header "Authorization: Bearer $MDC_API_KEY")

  PRESIGNED_URL="$(printf '%s' "$PRESIGNED_RESPONSE" | jq -r '.url // .presignedUrl')"

  if [ -z "$PRESIGNED_URL" ] || [ "$PRESIGNED_URL" = "null" ]; then
    echo "Missing presigned URL for part $PART_NUMBER" >&2
    exit 1
  fi

  HEADERS_FILE="$(mktemp)"

  dd if="$FILE_PATH" bs="$PART_SIZE" skip=$((PART_NUMBER - 1)) count=1 2>/dev/null | \
    curl --silent --show-error --fail \
      --request PUT \
      --data-binary @- \
      --dump-header "$HEADERS_FILE" \
      --output /dev/null \
      "$PRESIGNED_URL"

  ETAG="$(grep -i '^ETag:' "$HEADERS_FILE" | tail -n 1 | cut -d' ' -f2- | tr -d '\r')"
  rm -f "$HEADERS_FILE"

  if [ -z "$ETAG" ]; then
    echo "Could not read ETag for part $PART_NUMBER" >&2
    exit 1
  fi

  PARTS_JSON="$(printf '%s' "$PARTS_JSON" | jq \
    --argjson partNumber "$PART_NUMBER" \
    --arg etag "$ETAG" \
    '. + [{partNumber: $partNumber, etag: $etag}]')"

  PART_NUMBER=$((PART_NUMBER + 1))
done

COMPLETE_RESPONSE=$(curl --silent --show-error --fail \
  --request POST \
  --url "$API_BASE/uploads/$FILE_UPLOAD_ID" \
  --header "Authorization: Bearer $MDC_API_KEY" \
  --header 'Content-Type: application/json' \
  --data "$(jq -n \
    --arg uploadId "$UPLOAD_ID" \
    --arg checksum "$CHECKSUM" \
    --argjson parts "$PARTS_JSON" \
    '{uploadId: $uploadId, checksum: $checksum, parts: $parts}')")

log "Upload completed"
log "Completion response: $(printf '%s' "$COMPLETE_RESPONSE" | jq -c '.')"

printf '%s\n' "$FILE_UPLOAD_ID"
EOF

chmod +x upload_mdc_file.sh
```

Run it like this:

```bash
export FILE_UPLOAD_ID="$(./upload_mdc_file.sh "$FILE_PATH" "$SUBMISSION_ID")"
echo "Uploaded file. fileUploadId: $FILE_UPLOAD_ID"
```

### Notes about the raw upload flow

- The upload request must use `application/gzip`
- The part size comes from the API response to `POST /uploads`
- The `ETag` from each uploaded part must be preserved and sent back when completing the upload
- The final completion request must include the full-file SHA-256 checksum
- The presigned upload URLs are storage URLs, so the `PUT` requests go directly to those URLs rather than back to the MDC API

## Step 3: Update submission metadata and attach the uploaded file

Once the file upload is complete, update the draft submission metadata and set `fileUploadId` so the uploaded archive is attached to the submission.

```bash
UPDATE_RESPONSE=$(curl --silent --show-error --fail \
  --request PATCH \
  --url "$API_BASE/submissions/$SUBMISSION_ID" \
  --header "Authorization: Bearer $MDC_API_KEY" \
  --header 'Content-Type: application/json' \
  --data "$(jq -n \
    --arg fileUploadId "$FILE_UPLOAD_ID" \
    '{
      fileUploadId: $fileUploadId,
      shortDescription: "A brief description of the dataset.",
      longDescription: "A detailed description of the dataset.",
      locale: "en-US",
      task: "ASR",
      format: "TSV",
      licenseAbbreviation: "CC-BY-4.0",
      other: "A detailed datasheet-style description of the dataset contents.",
      restrictions: "No restrictions.",
      forbiddenUsage: "Do not use for unlawful purposes.",
      additionalConditions: "No additional conditions.",
      pointOfContactFullName: "Jane Doe",
      pointOfContactEmail: "jane@example.com",
      fundedByFullName: "Funder Name",
      fundedByEmail: "funder@example.com",
      legalContactFullName: "Legal Name",
      legalContactEmail: "legal@example.com",
      createdByFullName: "Creator Name",
      createdByEmail: "creator@example.com",
      intendedUsage: "Describe the intended usage of the dataset.",
      ethicalReviewProcess: "Describe the ethical review process.",
      exclusivityOptOut: false
    }')")

printf '%s\n' "$UPDATE_RESPONSE" | jq
```

For a predefined license, pass the SPDX-style value in `licenseAbbreviation`, such as `"CC-BY-4.0"`, and do not send `license` or `licenseUrl`.

For a custom license, send a custom `licenseAbbreviation` string or leave it empty, and include `license` plus `licenseUrl` if applicable. For example:

```bash
jq -n '{
  licenseAbbreviation: "Custom",
  license: "Research-only internal license",
  licenseUrl: "https://example.com/license"
}'
```

For the full submission field list, see the [`DatasetSubmission` API reference](api.md#datacollective.models.DatasetSubmission).

## Step 4: Submit the submission for review

After the file is attached and the metadata is complete, finalize the submission:

```bash
SUBMIT_RESPONSE=$(curl --silent --show-error --fail \
  --request POST \
  --url "$API_BASE/submissions/$SUBMISSION_ID" \
  --header "Authorization: Bearer $MDC_API_KEY" \
  --header 'Content-Type: application/json' \
  --data '{"agreeToSubmit": true}')

printf '%s\n' "$SUBMIT_RESPONSE" | jq
printf 'Submission status: %s\n' "$(printf '%s' "$SUBMIT_RESPONSE" | jq -r '.submission.status')"
```

## Common failure cases

### 1. Invalid or missing API key

Symptoms:

- `curl` returns `403 Forbidden`
- API calls that require authentication fail immediately

Check that `MDC_API_KEY` is set correctly:

```bash
printf '%s\n' "$MDC_API_KEY"
```

### 2. Missing required metadata before final submission

Symptoms:

- the `PATCH /submissions/{submissionId}` request fails, or
- the final `POST /submissions/{submissionId}` request is rejected

Make sure you have attached `fileUploadId` and provided the required metadata fields expected by the platform for review.

### 3. The upload script cannot read an `ETag`

Symptoms:

- a part upload succeeds but the script exits with `Could not read ETag`

The multipart completion request needs the `ETag` for every uploaded part. If that header is missing or cannot be parsed, the upload cannot be completed safely.

### 4. Expired or invalid presigned URL

Symptoms:

- the `PUT` request to the presigned URL fails with a storage-layer error

Request a fresh presigned URL for that part and retry the upload. Presigned URLs are temporary by design.

### 5. Interrupted upload

Symptoms:

- the shell exits part-way through a large upload
- network failure during one of the part uploads

With the simple terminal workflow in this guide, you generally restart the upload flow manually. Unlike the Python SDK, this guide does not persist upload state automatically.

### 6. Wrong MIME type or file format

Symptoms:

- upload initiation or backend validation fails

This upload flow expects a dataset archive uploaded as `application/gzip`, typically a `.tar.gz` file.

### 7. Upload exceeds the platform size limit

Symptoms:

- local checks or API validation fail for very large archives

Dataset uploads must be **80 GB or smaller**.

## Limitations of the terminal workflow vs the Python SDK

Calling the API directly is transparent and flexible, but you are responsible for more of the workflow yourself.

| Area | Terminal workflow with `curl` | Python SDK |
|------|-------------------------------|------------|
| Input validation | You validate JSON payloads yourself | Pydantic validates `DatasetSubmission` inputs before requests are sent |
| Upload resume | No built-in resume in this guide | Automatically persists upload state and resumes interrupted multipart uploads |
| Multipart bookkeeping | You must request part URLs, upload each part, collect `ETag`s, and complete the upload manually | Handled automatically |
| Retries | You add your own retry logic | Retries transient part upload failures automatically |
| Progress reporting | Not included by default | Built-in progress bar support |
| File checks | You must check file size, file existence, and checksum handling yourself | Checks file existence, non-empty uploads, size limit, and checksum flow automatically |
| Metadata shaping | You build JSON payloads manually | Converts SDK models into the correct payload shape, including license handling |
| State cleanup | You must manage temporary files and partial state yourself | Removes the upload state file automatically after success |
| Convenience | Multiple commands and shell helpers | One-step `create_submission_with_upload(...)` workflow is available |

If you want the lowest-level control or need to work outside Python, the raw API approach is useful. If you want validation, retries, resumable uploads, and a simpler interface, prefer the Python SDK guide in [Programmatic Uploads](upload.md).
