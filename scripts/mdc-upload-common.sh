#!/usr/bin/env bash

MDC_DEFAULT_API_BASE="https://datacollective.mozillafoundation.org/api"
MDC_MAX_UPLOAD_BYTES=80000000000

mdc_api_base() {
  printf '%s\n' "${MDC_API_URL:-$MDC_DEFAULT_API_BASE}"
}

mdc_log() {
  local level="$1"
  shift
  printf '[mdc][%s] %s\n' "$level" "$*" >&2
}

mdc_die() {
  mdc_log ERROR "$*"
  exit 1
}

mdc_require_env() {
  local name="$1"
  [[ -n "${!name:-}" ]] || mdc_die "$name is required"
}

mdc_require_tools() {
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || mdc_die "Required tool not found: $tool"
  done
}

mdc_require_file() {
  local path="$1"
  [[ -f "$path" ]] || mdc_die "File not found: $path"
}

mdc_file_size() {
  local path="$1"
  wc -c < "$path" | tr -d ' '
}

mdc_validate_upload_file() {
  local path="$1"
  local size

  mdc_require_file "$path"
  size="$(mdc_file_size "$path")"

  [[ "$size" =~ ^[0-9]+$ ]] || mdc_die "Could not determine file size for $path"
  (( size > 0 )) || mdc_die "File must be non-empty: $path"
  (( size <= MDC_MAX_UPLOAD_BYTES )) || mdc_die "File exceeds the 80 GB upload limit: $path"
}

mdc_compute_sha256() {
  local path="$1"

  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
    return
  fi

  mdc_die "Could not find sha256sum or shasum"
}

mdc_api_request_json() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local url

  url="$(mdc_api_base)${path}"

  if [[ -n "$body" ]]; then
    curl --silent --show-error --fail \
      --request "$method" \
      --url "$url" \
      --header "Authorization: Bearer $MDC_API_KEY" \
      --header 'Content-Type: application/json' \
      --data "$body"
  else
    curl --silent --show-error --fail \
      --request "$method" \
      --url "$url" \
      --header "Authorization: Bearer $MDC_API_KEY"
  fi
}

mdc_json_get() {
  local expr="$1"
  local input="${2:-}"

  if [[ -n "$input" ]]; then
    printf '%s' "$input" | jq -er "$expr"
  else
    jq -er "$expr"
  fi
}

mdc_json_compact() {
  jq -c '.'
}

mdc_print_json() {
  local input="$1"
  printf '%s\n' "$input" | jq '.'
}

mdc_trim_etag() {
  local etag="$1"
  etag="${etag%\"}"
  etag="${etag#\"}"
  printf '%s\n' "$etag"
}

