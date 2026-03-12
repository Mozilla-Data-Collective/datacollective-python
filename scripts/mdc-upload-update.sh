#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./mdc-upload-common.sh
source "$SCRIPT_DIR/mdc-upload-common.sh"

usage() {
  cat <<'EOF'
Update submission metadata and optionally attach a file upload.

Usage:
  bash scripts/mdc-upload-update.sh --submission-id <id> --metadata-file submission.json [--file-upload-id <id>] [--output id|json]

Options:
  --submission-id   Draft submission ID
  --metadata-file   Path to a JSON file containing DatasetSubmission fields
  --file-upload-id  Uploaded file ID to inject into the PATCH payload
  --output          Output format: json (default) or id
  --help            Show this help message
EOF
}

SUBMISSION_ID=""
METADATA_FILE=""
FILE_UPLOAD_ID=""
OUTPUT="json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --submission-id)
      SUBMISSION_ID="${2:-}"
      shift 2
      ;;
    --metadata-file)
      METADATA_FILE="${2:-}"
      shift 2
      ;;
    --file-upload-id)
      FILE_UPLOAD_ID="${2:-}"
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

[[ -n "$SUBMISSION_ID" ]] || mdc_die "--submission-id is required"
[[ -n "$METADATA_FILE" ]] || mdc_die "--metadata-file is required"
[[ "$OUTPUT" == "id" || "$OUTPUT" == "json" ]] || mdc_die "--output must be id or json"

mdc_require_tools curl jq
mdc_require_env MDC_API_KEY
mdc_require_file "$METADATA_FILE"

jq -e 'type == "object"' "$METADATA_FILE" >/dev/null || mdc_die "Metadata file must contain a JSON object"

if [[ -n "$FILE_UPLOAD_ID" ]]; then
  payload="$(jq --arg fileUploadId "$FILE_UPLOAD_ID" '. + {fileUploadId: $fileUploadId}' "$METADATA_FILE")"
else
  payload="$(cat "$METADATA_FILE")"
fi

response="$(mdc_api_request_json PATCH "/submissions/$SUBMISSION_ID" "$payload")"
updated_id="$(mdc_json_get '.submission.id' "$response")"

mdc_log INFO "Updated submission metadata: $updated_id"

if [[ "$OUTPUT" == "json" ]]; then
  mdc_print_json "$response"
else
  printf '%s\n' "$updated_id"
fi

