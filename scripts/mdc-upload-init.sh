#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./mdc-upload-common.sh
source "$SCRIPT_DIR/mdc-upload-common.sh"

usage() {
  cat <<'EOF'
Create a draft submission.

Usage:
  bash scripts/mdc-upload-init.sh --name "Dataset Name" [--output id|json]

Options:
  --name      Draft submission name
  --output    Output format: id (default) or json
  --help      Show this help message
EOF
}

NAME=""
OUTPUT="id"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)
      NAME="${2:-}"
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

[[ -n "$NAME" ]] || mdc_die "--name is required"
[[ "$OUTPUT" == "id" || "$OUTPUT" == "json" ]] || mdc_die "--output must be id or json"

mdc_require_tools curl jq
mdc_require_env MDC_API_KEY

payload="$(jq -n --arg name "$NAME" '{name: $name}')"
response="$(mdc_api_request_json POST '/submissions' "$payload")"
submission_id="$(mdc_json_get '.submission.id' "$response")"

mdc_log INFO "Created draft submission: $submission_id"

if [[ "$OUTPUT" == "json" ]]; then
  mdc_print_json "$response"
else
  printf '%s\n' "$submission_id"
fi

