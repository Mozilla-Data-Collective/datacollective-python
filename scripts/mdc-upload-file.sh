#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./mdc-upload-common.sh
source "$SCRIPT_DIR/mdc-upload-common.sh"

usage() {
  cat <<'EOF'
Upload a dataset archive to an existing draft submission.

Usage:
  bash scripts/mdc-upload-file.sh --file /path/to/dataset.tar.gz --submission-id <id> [--output id|json]

Options:
  --file            Path to the .tar.gz archive
  --submission-id   Draft submission ID
  --output          Output format: id (default) or json
  --help            Show this help message
EOF
}

FILE_PATH=""
SUBMISSION_ID=""
OUTPUT="id"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --file)
      FILE_PATH="${2:-}"
      shift 2
      ;;
    --submission-id)
      SUBMISSION_ID="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      mdc_die "Unknown argument: $1"
      ;;
  esac
done

[[ -n "$FILE_PATH" ]] || mdc_die "--file is required"
[[ -n "$SUBMISSION_ID" ]] || mdc_die "--submission-id is required"
[[ "$OUTPUT" == "id" || "$OUTPUT" == "json" ]] || mdc_die "--output must be id or json"

mdc_require_tools curl jq dd grep cut tr mktemp
mdc_require_env MDC_API_KEY
mdc_validate_upload_file "$FILE_PATH"

FILE_NAME="$(basename "$FILE_PATH")"
FILE_SIZE="$(mdc_file_size "$FILE_PATH")"
CHECKSUM="$(mdc_compute_sha256 "$FILE_PATH")"

mdc_log INFO "Starting upload for $FILE_NAME ($FILE_SIZE bytes)"
mdc_log INFO "Computing upload session"

init_payload="$(jq -n \
  --arg submissionId "$SUBMISSION_ID" \
  --arg filename "$FILE_NAME" \
  --arg mimeType 'application/gzip' \
  --argjson fileSize "$FILE_SIZE" \
  '{submissionId: $submissionId, filename: $filename, fileSize: $fileSize, mimeType: $mimeType}')"
init_response="$(mdc_api_request_json POST '/uploads' "$init_payload")"

FILE_UPLOAD_ID="$(mdc_json_get '.fileUploadId' "$init_response")"
UPLOAD_ID="$(mdc_json_get '.uploadId' "$init_response")"
PART_SIZE="$(mdc_json_get '.partSize' "$init_response")"
TOTAL_PARTS=$(( (FILE_SIZE + PART_SIZE - 1) / PART_SIZE ))
PARTS_JSON='[]'

mdc_log INFO "Upload session created: fileUploadId=$FILE_UPLOAD_ID uploadId=$UPLOAD_ID partSize=$PART_SIZE totalParts=$TOTAL_PARTS"

PART_NUMBER=1
while [[ "$PART_NUMBER" -le "$TOTAL_PARTS" ]]; do
  mdc_log INFO "Uploading part $PART_NUMBER/$TOTAL_PARTS"

  presigned_response="$(mdc_api_request_json GET "/uploads/$FILE_UPLOAD_ID/parts/$PART_NUMBER")"
  presigned_url="$(printf '%s' "$presigned_response" | jq -er '.url // .presignedUrl')"
  headers_file="$(mktemp)"

  dd if="$FILE_PATH" bs="$PART_SIZE" skip=$((PART_NUMBER - 1)) count=1 2>/dev/null | \
    curl --silent --show-error --fail \
      --request PUT \
      --data-binary @- \
      --dump-header "$headers_file" \
      --output /dev/null \
      "$presigned_url"

  raw_etag="$(grep -i '^ETag:' "$headers_file" | tail -n 1 | cut -d' ' -f2- | tr -d '\r')"
  rm -f "$headers_file"

  [[ -n "$raw_etag" ]] || mdc_die "Could not read ETag for part $PART_NUMBER"
  etag="$(mdc_trim_etag "$raw_etag")"

  PARTS_JSON="$(printf '%s' "$PARTS_JSON" | jq \
    --argjson partNumber "$PART_NUMBER" \
    --arg etag "$etag" \
    '. + [{partNumber: $partNumber, etag: $etag}]')"

  PART_NUMBER=$((PART_NUMBER + 1))
done

complete_payload="$(jq -n \
  --arg uploadId "$UPLOAD_ID" \
  --arg checksum "$CHECKSUM" \
  --argjson parts "$PARTS_JSON" \
  '{uploadId: $uploadId, checksum: $checksum, parts: $parts}')"
complete_response="$(mdc_api_request_json POST "/uploads/$FILE_UPLOAD_ID" "$complete_payload")"

mdc_log INFO "Upload complete: fileUploadId=$FILE_UPLOAD_ID"

if [[ "$OUTPUT" == "json" ]]; then
  output_json="$(jq -n \
    --arg fileUploadId "$FILE_UPLOAD_ID" \
    --arg uploadId "$UPLOAD_ID" \
    --arg checksum "$CHECKSUM" \
    --argjson apiResponse "$complete_response" \
    '{fileUploadId: $fileUploadId, uploadId: $uploadId, checksum: $checksum, completeResponse: $apiResponse}')"
  mdc_print_json "$output_json"
else
  printf '%s\n' "$FILE_UPLOAD_ID"
fi

