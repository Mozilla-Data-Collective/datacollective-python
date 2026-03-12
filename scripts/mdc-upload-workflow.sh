#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./mdc-upload-common.sh
source "$SCRIPT_DIR/mdc-upload-common.sh"

usage() {
  cat <<'EOF'
Run the full terminal upload workflow: create draft, upload file, update metadata, and submit.

Usage:
  bash scripts/mdc-upload-workflow.sh --file /path/to/dataset.tar.gz --metadata-file submission.json [--output json|summary]

Options:
  --file            Path to the dataset archive
  --metadata-file   JSON file containing DatasetSubmission fields (must include .name)
  --output          Output format: json (default) or summary
  --help            Show this help message
EOF
}

FILE_PATH=""
METADATA_FILE=""
OUTPUT="json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --file)
      FILE_PATH="${2:-}"
      shift 2
      ;;
    --metadata-file)
      METADATA_FILE="${2:-}"
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
[[ -n "$METADATA_FILE" ]] || mdc_die "--metadata-file is required"
[[ "$OUTPUT" == "json" || "$OUTPUT" == "summary" ]] || mdc_die "--output must be json or summary"

mdc_require_tools bash curl jq dd grep cut tr mktemp
mdc_require_env MDC_API_KEY
mdc_require_file "$METADATA_FILE"
mdc_validate_upload_file "$FILE_PATH"

jq -e 'type == "object" and (.name | type == "string") and (.name | length > 0)' "$METADATA_FILE" >/dev/null \
  || mdc_die "Metadata file must contain a non-empty string field: name"

DATASET_NAME="$(jq -r '.name' "$METADATA_FILE")"

mdc_log INFO "Creating draft submission"
SUBMISSION_ID="$(bash "$SCRIPT_DIR/mdc-upload-init.sh" --name "$DATASET_NAME")"

mdc_log INFO "Uploading archive"
FILE_UPLOAD_ID="$(bash "$SCRIPT_DIR/mdc-upload-file.sh" --file "$FILE_PATH" --submission-id "$SUBMISSION_ID")"

mdc_log INFO "Updating submission metadata"
bash "$SCRIPT_DIR/mdc-upload-update.sh" \
  --submission-id "$SUBMISSION_ID" \
  --metadata-file "$METADATA_FILE" \
  --file-upload-id "$FILE_UPLOAD_ID" \
  --output id >/dev/null

mdc_log INFO "Submitting draft for review"
FINAL_STATUS="$(bash "$SCRIPT_DIR/mdc-upload-submit.sh" --submission-id "$SUBMISSION_ID" --output status)"

if [[ "$OUTPUT" == "summary" ]]; then
  printf 'submissionId=%s\n' "$SUBMISSION_ID"
  printf 'fileUploadId=%s\n' "$FILE_UPLOAD_ID"
  printf 'status=%s\n' "$FINAL_STATUS"
else
  jq -n \
    --arg submissionId "$SUBMISSION_ID" \
    --arg fileUploadId "$FILE_UPLOAD_ID" \
    --arg status "$FINAL_STATUS" \
    '{submissionId: $submissionId, fileUploadId: $fileUploadId, status: $status}'
fi

