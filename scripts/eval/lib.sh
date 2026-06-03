#!/usr/bin/env bash
set -euo pipefail

eval_die() {
  echo "$1" >&2
  exit "${2:-1}"
}

eval_usage_error() {
  local script_name="$1"
  local message="$2"
  echo "$script_name: $message" >&2
  echo "Try '$script_name --help' for usage." >&2
  exit 2
}

eval_require_value() {
  local script_name="$1"
  local flag="$2"
  local value="${3:-}"
  if [[ -z "$value" ]]; then
    eval_usage_error "$script_name" "missing value for $flag"
  fi
}

eval_repo_root() {
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  cd "$script_dir/../.." && pwd
}

eval_timestamp_utc() {
  date -u +"%Y%m%dT%H%M%SZ"
}

eval_safe_component() {
  local value="$1"
  local safe
  safe="$(printf '%s' "$value" | tr -c 'A-Za-z0-9_.=-' '-')"
  if [[ -z "$safe" ]]; then
    safe="unknown"
  fi
  printf '%s\n' "$safe"
}

eval_run_id() {
  local run_id
  if [[ -n "${EVAL_RUN_ID:-}" ]]; then
    run_id="$EVAL_RUN_ID"
  else
    run_id="$(eval_timestamp_utc)"
  fi
  eval_safe_component "$run_id"
}

eval_safe_label() {
  local label="$1"
  eval_safe_component "$label"
}

eval_prepare_out_dir() {
  local phase="$1"
  local node_label="$2"
  local requested_out_dir="${3:-}"
  local run_id="${4:-}"
  local out_dir

  if [[ -n "$requested_out_dir" ]]; then
    out_dir="$requested_out_dir"
  else
    if [[ -z "$run_id" ]]; then
      run_id="$(eval_run_id)"
    fi
    out_dir="$(eval_repo_root)/artifacts/evaluation/$run_id/$(eval_safe_label "$node_label")/$phase"
  fi

  mkdir -p "$out_dir"
  printf '%s\n' "$out_dir"
}

eval_resolve_entmootd() {
  local wrapper="${ENTMOOT_AGENT_WRAPPER:-/data/.entmoot/entmoot}"
  if [[ -n "${ENTMOOTD:-}" ]]; then
    printf '%s\n' "$ENTMOOTD"
  elif [[ -x "$wrapper" ]]; then
    printf '%s\n' "$wrapper"
  else
    printf '%s\n' entmootd
  fi
}

eval_require_command() {
  local command_name="$1"
  if [[ "$command_name" == */* ]]; then
    [[ -x "$command_name" ]] || eval_die "command is not executable: $command_name"
  else
    command -v "$command_name" >/dev/null 2>&1 || eval_die "command not found: $command_name"
  fi
}

eval_entmoot_base=()

eval_init_entmoot() {
  local entmootd_bin
  entmootd_bin="$(eval_resolve_entmootd)"
  eval_require_command "$entmootd_bin"

  eval_entmoot_base=("$entmootd_bin")
  if [[ -n "${PILOT_SOCKET:-}" ]]; then
    eval_entmoot_base+=("-socket" "$PILOT_SOCKET")
  fi
  if [[ -n "${ENTMOOT_IDENTITY:-}" ]]; then
    eval_entmoot_base+=("-identity" "$ENTMOOT_IDENTITY")
  fi
  if [[ -n "${ENTMOOT_DATA:-}" ]]; then
    eval_entmoot_base+=("-data" "$ENTMOOT_DATA")
  fi
}

eval_write_command_meta() {
  local meta_path="$1"
  local label="$2"
  local start_ts="$3"
  local end_ts="$4"
  local status="$5"
  shift 5

  {
    printf 'label=%s\n' "$label"
    printf 'start_utc=%s\n' "$start_ts"
    printf 'end_utc=%s\n' "$end_ts"
    printf 'exit_status=%s\n' "$status"
    printf 'command='
    printf '%q ' "$@"
    printf '\n'
  } >"$meta_path"
}

eval_run_capture() {
  local label="$1"
  local out_dir="$2"
  shift 2

  local stdout_path="$out_dir/$label.stdout"
  local stderr_path="$out_dir/$label.stderr"
  local status_path="$out_dir/$label.status"
  local meta_path="$out_dir/$label.meta"
  local start_ts end_ts status

  start_ts="$(eval_timestamp_utc)"
  if "$@" >"$stdout_path" 2>"$stderr_path"; then
    status=0
  else
    status=$?
  fi
  end_ts="$(eval_timestamp_utc)"

  printf '%s\n' "$status" >"$status_path"
  eval_write_command_meta "$meta_path" "$label" "$start_ts" "$end_ts" "$status" "$@"
  return "$status"
}

eval_run_capture_stdin() {
  local label="$1"
  local out_dir="$2"
  local input="$3"
  shift 3

  local stdout_path="$out_dir/$label.stdout"
  local stderr_path="$out_dir/$label.stderr"
  local status_path="$out_dir/$label.status"
  local meta_path="$out_dir/$label.meta"
  local start_ts end_ts status

  start_ts="$(eval_timestamp_utc)"
  if printf '%s' "$input" | "$@" >"$stdout_path" 2>"$stderr_path"; then
    status=0
  else
    status=$?
  fi
  end_ts="$(eval_timestamp_utc)"

  printf '%s\n' "$status" >"$status_path"
  eval_write_command_meta "$meta_path" "$label" "$start_ts" "$end_ts" "$status" "$@"
  return "$status"
}

eval_sha256_text() {
  local value="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$value" | sha256sum | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    printf '%s' "$value" | shasum -a 256 | awk '{print $1}'
  else
    printf '%s\n' unavailable
  fi
}

eval_write_run_metadata() {
  local out_dir="$1"
  local node_label="$2"
  local phase="$3"
  local group_id="$4"
  local probe="$5"
  local timeout_value="$6"
  local run_id="$7"

  {
    printf 'run_id=%s\n' "$run_id"
    printf 'phase=%s\n' "$phase"
    printf 'node_label=%s\n' "$node_label"
    printf 'group_id=%s\n' "$group_id"
    printf 'probe=%s\n' "$probe"
    printf 'timeout=%s\n' "$timeout_value"
    printf 'created_utc=%s\n' "$(eval_timestamp_utc)"
    printf 'repo_root=%s\n' "$(eval_repo_root)"
    printf 'entmoot_base='
    printf '%q ' "${eval_entmoot_base[@]}"
    printf '\n'
  } >"$out_dir/run.meta"
}
