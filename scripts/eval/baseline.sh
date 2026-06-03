#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/eval/lib.sh
source "$script_dir/lib.sh"

script_name="$(basename "$0")"
group_id=""
node_label=""
out_dir=""
probe=0
probe_timeout="3s"

usage() {
  cat <<'USAGE'
Usage: baseline.sh --group GROUP_ID --node-label LABEL [--out-dir DIR] [--probe] [--timeout DURATION]

Capture an Entmoot runtime baseline for the live-agent evaluation. The script
writes command stdout, stderr, exit status, and metadata under an ignored
artifacts/evaluation run directory unless --out-dir is provided.

Options:
  --group GROUP_ID       Entmoot group id to diagnose
  --node-label LABEL     stable operator label for this runtime
  --out-dir DIR          output directory override
  --probe                actively probe peers during doctor and peers checks
  --timeout DURATION     per-peer probe timeout (default: 3s)
  -h, --help             show this help

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
    --probe)
      probe=1
      shift
      ;;
    --timeout)
      eval_require_value "$script_name" "$1" "${2:-}"
      probe_timeout="$2"
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

[[ -n "$group_id" ]] || eval_usage_error "$script_name" "--group is required"
[[ -n "$node_label" ]] || eval_usage_error "$script_name" "--node-label is required"

eval_init_entmoot
run_id="$(eval_run_id)"
out_dir="$(eval_prepare_out_dir baseline "$node_label" "$out_dir" "$run_id")"
eval_write_run_metadata "$out_dir" "$node_label" "baseline" "$group_id" "$probe" "$probe_timeout" "$run_id"

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

doctor_cmd=("${eval_entmoot_base[@]}" doctor -group "$group_id" --json)
peers_cmd=("${eval_entmoot_base[@]}" peers -group "$group_id" --json)
if (( probe )); then
  doctor_cmd+=(--probe --timeout "$probe_timeout")
  peers_cmd+=(--probe --timeout "$probe_timeout")
fi

run_required env "${eval_entmoot_base[@]}" env --json
run_required info "${eval_entmoot_base[@]}" info
run_required doctor "${doctor_cmd[@]}"
run_required peers "${peers_cmd[@]}"

echo
echo "baseline output: $out_dir"

if (( failures > 0 )); then
  echo "baseline.sh: $failures command(s) failed; captured output remains in $out_dir" >&2
  exit 1
fi
