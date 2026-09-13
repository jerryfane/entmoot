#!/usr/bin/env bash
set -euo pipefail

agent_wrapper="${ENTMOOT_AGENT_WRAPPER:-/data/.entmoot/entmoot}"
if [[ -n "${ENTMOOTD:-}" ]]; then
  entmootd_bin="$ENTMOOTD"
elif [[ -x "$agent_wrapper" ]]; then
  entmootd_bin="$agent_wrapper"
else
  entmootd_bin="entmootd"
fi

usage() {
  cat <<'USAGE'
Usage: verify-agent-runtime.sh [--group GROUP_ID] [--probe] [--timeout DURATION]

Read-only verification for an Entmoot libp2p runtime. The check fails unless
`entmootd env --json` reports a reachable publish path. With --group, it also
runs the group doctor and requires every returned diagnosis to be healthy.

Environment:
  ENTMOOTD              entmootd or wrapper path
  ENTMOOT_AGENT_WRAPPER optional deployed wrapper path
  ENTMOOT_DATA          optional Entmoot data root
  ENTMOOT_IDENTITY      optional Entmoot identity path
USAGE
}

group_id=""
probe=0
probe_timeout="3s"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --group) group_id="${2:?missing value for --group}"; shift 2 ;;
    --probe) probe=1; shift ;;
    --timeout) probe_timeout="${2:?missing value for --timeout}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "verify-agent-runtime.sh: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

json_string_field() {
  local field="$1"
  sed -nE 's/.*"'"$field"'":"([^"]*)".*/\1/p'
}

json_bool_field() {
  local field="$1"
  sed -nE 's/.*"'"$field"'":(true|false).*/\1/p'
}

entmoot_base=("$entmootd_bin")
if [[ -n "${ENTMOOT_IDENTITY:-}" ]]; then
  entmoot_base+=("-identity" "$ENTMOOT_IDENTITY")
fi
if [[ -n "${ENTMOOT_DATA:-}" ]]; then
  entmoot_base+=("-data" "$ENTMOOT_DATA")
fi

echo "== entmoot runtime env =="
if ! report="$("${entmoot_base[@]}" env --json 2>&1)"; then
  printf '%s\n' "$report" >&2
  exit 1
fi
runtime_status="$(printf '%s\n' "$report" | json_string_field runtime_status)"
publish_path_healthy="$(printf '%s\n' "$report" | json_bool_field publish_path_healthy)"
control_socket_reachable="$(printf '%s\n' "$report" | json_bool_field control_socket_reachable)"
echo "runtime_status: ${runtime_status:-unknown}"
echo "publish_path_healthy: ${publish_path_healthy:-unknown}"
echo "control_socket_reachable: ${control_socket_reachable:-unknown}"
if [[ "$publish_path_healthy" != "true" ]]; then
  echo "verify-agent-runtime.sh: publish path is not healthy" >&2
  printf '%s\n' "$report" >&2
  exit 1
fi

if [[ -n "$group_id" ]]; then
  echo
  echo "== entmoot doctor =="
  doctor_cmd=("${entmoot_base[@]}" doctor -group "$group_id" --json)
  if (( probe )); then
    doctor_cmd+=(--probe --timeout "$probe_timeout")
  fi
  doctor_report="$("${doctor_cmd[@]}")"
  printf '%s\n' "$doctor_report"
  if printf '%s\n' "$doctor_report" | grep -q '"diagnosis":"[^o]'; then
    echo "verify-agent-runtime.sh: unhealthy peer diagnosis" >&2
    exit 1
  fi
fi
