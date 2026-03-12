#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./mdc-upload-common.sh
source "$SCRIPT_DIR/mdc-upload-common.sh"

usage() {
  cat <<'EOF'
Submit a draft submission for review.

Usage:
  bash scripts/mdc-upload-submit.sh --submission-id <id> [--output status|id|json]

Options:
  --submission-id   Draft submission ID
  --output          Output format: status (default), id, or json
  --help            Show this help message
EOF
}

SUBMISSION_ID=""
OUTPUT="status"

while [[ $# -gt 0 ]]; do
  case "$1" in
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

[[ -n "$SUBMISSION_ID" ]] || mdc_die "--submission-id is required"
case "$OUTPUT" in
  status|id|json) ;;
  *) mdc_die "--output must be status, id, or json" ;;
esac

mdc_require_tools curl jq
mdc_require_env MDC_API_KEY

payload='{"agreeToSubmit": true}'
response="$(mdc_api_request_json POST "/submissions/$SUBMISSION_ID" "$payload")"
submission_id="$(mdc_json_get '.submission.id' "$response")"
status="$(mdc_json_get '.submission.status' "$response")"

mdc_log INFO "Submission finalized: id=$submission_id status=$status"

case "$OUTPUT" in
  json)
    mdc_print_json "$response"
    ;;
  id)
    printf '%s\n' "$submission_id"
    ;;
  status)
    printf '%s\n' "$status"
    ;;
esac

