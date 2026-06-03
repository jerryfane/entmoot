#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/eval/lib.sh
source "$script_dir/lib.sh"

script_name="$(basename "$0")"
invite_path=""
group_id=""
node_label=""
out_dir=""

usage() {
  cat <<'USAGE'
Usage: join-controlled-moot.sh --invite PATH --node-label LABEL [--group GROUP_ID] [--out-dir DIR]

Join the controlled evaluation moot from the current Entmoot runtime and capture
join plus post-join diagnostic output. Run this inside the runtime/container
that owns the node identity. Do not commit invite files or captured raw output.

Options:
  --invite PATH       signed invite JSON path or supported invite descriptor/link
  --group GROUP_ID    expected group id for post-join doctor diagnostics
  --node-label LABEL  stable operator label for this runtime
  --out-dir DIR       output directory override
  -h, --help          show this help

Environment:
  EVAL_RUN_ID            optional run id for default artifact paths
  ENTMOOTD               entmootd or wrapper path
  ENTMOOT_AGENT_WRAPPER  optional wrapper path (default: /data/.entmoot/entmoot)
  ENTMOOT_DATA           optional Entmoot data root passed as -data
  ENTMOOT_IDENTITY       optional Entmoot identity path passed as -identity
  PILOT_SOCKET           optional Pilot IPC socket path passed as -socket
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --invite)
      eval_require_value "$script_name" "$1" "${2:-}"
      invite_path="$2"
      shift 2
      ;;
    --group)
      eval_require_value "$script_name" "$1" "${2:-}"
      group_id="$2"
      shift 2
      ;;
    --node-label)
      eval_require_value "$script_name" "$1" "${2:-}"
      node_label="$2"
      shift 2
      ;;
    --out-dir)
      eval_require_value "$script_name" "$1" "${2:-}"
      out_dir="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      eval_usage_error "$script_name" "unknown argument: $1"
      ;;
  esac
done

[[ -n "$invite_path" ]] || eval_usage_error "$script_name" "--invite is required"
[[ -n "$node_label" ]] || eval_usage_error "$script_name" "--node-label is required"

if [[ "$invite_path" != entmoot://* && "$invite_path" != http://* && "$invite_path" != https://* ]]; then
  [[ -f "$invite_path" ]] || eval_die "invite file not found: $invite_path"
  [[ -s "$invite_path" ]] || eval_die "invite file is empty: $invite_path"
fi

eval_init_entmoot
run_id="$(eval_run_id)"
out_dir="$(eval_prepare_out_dir join "$node_label" "$out_dir" "$run_id")"
invite_input_kind="file"
invite_input_label="$(basename "$invite_path")"
if [[ "$invite_path" == entmoot://* ]]; then
  invite_input_kind="entmoot_link"
  invite_input_label="redacted"
elif [[ "$invite_path" == http://* || "$invite_path" == https://* ]]; then
  invite_input_kind="url"
  invite_input_label="redacted"
fi

{
  printf 'run_id=%s\n' "$run_id"
  printf 'phase=join\n'
  printf 'node_label=%s\n' "$node_label"
  printf 'group_id=%s\n' "$group_id"
  printf 'invite_input_kind=%s\n' "$invite_input_kind"
  printf 'invite_input_label=%s\n' "$invite_input_label"
  printf 'created_utc=%s\n' "$(eval_timestamp_utc)"
  printf 'entmoot_base='
  printf '%q ' "${eval_entmoot_base[@]}"
  printf '\n'
} >"$out_dir/run.meta"

failures=0

run_required() {
  local label="$1"
  shift
  echo "== $label =="
  if eval_run_capture "$label" "$out_dir" "$@"; then
    echo "ok: $label"
  else
    local status=$?
    echo "failed: $label (exit $status)" >&2
    failures=$((failures + 1))
  fi
}

run_join() {
  local label="join"
  local stdout_path="$out_dir/$label.stdout"
  local stderr_path="$out_dir/$label.stderr"
  local status_path="$out_dir/$label.status"
  local meta_path="$out_dir/$label.meta"
  local redacted_invite="<invite:$invite_input_kind:$invite_input_label>"
  local start_ts end_ts status

  echo "== $label =="
  start_ts="$(eval_timestamp_utc)"
  if "${eval_entmoot_base[@]}" join "$invite_path" >"$stdout_path" 2>"$stderr_path"; then
    status=0
  else
    status=$?
  fi
  end_ts="$(eval_timestamp_utc)"
  printf '%s\n' "$status" >"$status_path"
  eval_write_command_meta "$meta_path" "$label" "$start_ts" "$end_ts" "$status" \
    "${eval_entmoot_base[@]}" join "$redacted_invite"

  if [[ "$status" == "0" ]]; then
    echo "ok: $label"
  else
    echo "failed: $label (exit $status)" >&2
    failures=$((failures + 1))
  fi
}

run_join
run_required env "${eval_entmoot_base[@]}" env --json
run_required info "${eval_entmoot_base[@]}" info

if [[ -n "$group_id" ]]; then
  run_required doctor "${eval_entmoot_base[@]}" doctor -group "$group_id" --json
else
  echo "skipped: doctor (--group not provided)"
fi

echo
echo "join output: $out_dir"

if (( failures > 0 )); then
  echo "join-controlled-moot.sh: $failures command(s) failed; captured output remains in $out_dir" >&2
  exit 1
fi
