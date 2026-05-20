#!/usr/bin/env bash
set -euo pipefail

default_entmootd() {
  if [[ -n "${ENTMOOTD:-}" ]]; then
    printf '%s\n' "$ENTMOOTD"
  elif [[ -x "$agent_wrapper" ]]; then
    printf '%s\n' "$agent_wrapper"
  else
    printf '%s\n' entmootd
  fi
}

agent_wrapper="${ENTMOOT_AGENT_WRAPPER:-/data/.entmoot/entmoot}"
entmootd_bin="$(default_entmootd)"
stack_helper="${ENTMOOT_STACK_HELPER:-/data/.pilot/start-entmoot-stack.sh}"

usage() {
  cat <<'USAGE'
Usage: verify-agent-runtime.sh

Read-only verification for an agent/container Entmoot runtime. The check fails
unless `entmootd env --json` reports a reachable direct publish path.
When `--group` is provided, it also runs `doctor` for that group and fails on
any non-ok peer diagnosis.

Options:
  --group GROUP_ID       run doctor for this group after env/stack checks
  --probe                actively probe routes during the doctor check
  --timeout DURATION     per-peer doctor probe timeout (default: 3s)

Environment:
  ENTMOOTD              entmootd or wrapper path (default: /data/.entmoot/entmoot when present, else entmootd)
  ENTMOOT_AGENT_WRAPPER optional deployed wrapper path (default: /data/.entmoot/entmoot)
  ENTMOOT_DATA          optional Entmoot data root passed as -data
  ENTMOOT_IDENTITY      optional Entmoot identity path passed as -identity
  PILOT_SOCKET          optional Pilot IPC socket path passed as -socket
  ENTMOOT_STACK_HELPER  optional stack helper path (default: /data/.pilot/start-entmoot-stack.sh)
USAGE
}

group_id=""
probe=0
probe_timeout="3s"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --group)
      group_id="${2:?missing value for --group}"
      shift 2
      ;;
    --probe)
      probe=1
      shift
      ;;
    --timeout)
      probe_timeout="${2:?missing value for --timeout}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "verify-agent-runtime.sh: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
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
if [[ -n "${PILOT_SOCKET:-}" ]]; then
  entmoot_base+=("-socket" "$PILOT_SOCKET")
fi
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
pilot_socket_reachable="$(printf '%s\n' "$report" | json_bool_field pilot_socket_reachable)"
control_socket_reachable="$(printf '%s\n' "$report" | json_bool_field control_socket_reachable)"

echo "runtime_status: ${runtime_status:-unknown}"
echo "publish_path_healthy: ${publish_path_healthy:-unknown}"
echo "pilot_socket_reachable: ${pilot_socket_reachable:-unknown}"
echo "control_socket_reachable: ${control_socket_reachable:-unknown}"

if [[ "$publish_path_healthy" != "true" ]]; then
  echo "verify-agent-runtime.sh: direct publish path is not healthy" >&2
  printf '%s\n' "$report" >&2
  if [[ -x "$stack_helper" ]]; then
    echo "hint: run read-only stack check: $stack_helper check" >&2
  fi
  exit 1
fi

if [[ -x "$stack_helper" ]]; then
  echo
  echo "== stack helper check =="
  "$stack_helper" check
else
  echo
  echo "stack helper not executable or not present: $stack_helper"
fi

if [[ -n "$group_id" ]]; then
  echo
  echo "== entmoot doctor =="
  doctor_cmd=("${entmoot_base[@]}" doctor -group "$group_id" --json)
  if (( probe )); then
    doctor_cmd+=(--probe --timeout "$probe_timeout")
  fi
  if ! doctor_report="$("${doctor_cmd[@]}" 2>&1)"; then
    printf '%s\n' "$doctor_report" >&2
    exit 1
  fi
  diagnoses="$(
    printf '%s\n' "$doctor_report" \
      | grep -o '"diagnosis":"[^"]*"' \
      | cut -d: -f2 \
      | tr -d '"' || true
  )"
  diagnosis_count="$(
    printf '%s\n' "$diagnoses" \
      | grep -Ec '.+' || true
  )"
  if [[ "$diagnosis_count" == "0" ]]; then
    echo "verify-agent-runtime.sh: doctor checked no peer diagnoses for group $group_id" >&2
    printf '%s\n' "$doctor_report" >&2
    exit 1
  fi
  bad_diagnoses="$(
    printf '%s\n' "$diagnoses" \
      | grep -Ev '^ok$' \
      | sort -u \
      | tr '\n' ' ' \
      | sed 's/[[:space:]]*$//' || true
  )"
  if [[ -n "$bad_diagnoses" ]]; then
    echo "verify-agent-runtime.sh: unhealthy doctor diagnoses: $bad_diagnoses" >&2
    printf '%s\n' "$doctor_report" >&2
    exit 1
  fi
  echo "doctor_diagnoses: ok"
fi
