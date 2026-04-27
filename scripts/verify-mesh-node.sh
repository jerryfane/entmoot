#!/usr/bin/env bash
set -euo pipefail

limit=1000
log_minutes=10

usage() {
  cat <<'USAGE'
Usage: verify-mesh-node.sh [--limit n] [--log-minutes n]

Prints a local mesh health snapshot after Pilot/Entmoot restart.

Environment:
  PILOTCTL      pilotctl binary path (default: pilotctl)
  ENTMOOTD      entmootd binary path (default: entmootd)
  GROUP         optional Entmoot group id for query
  PILOT_SOCKET  optional Pilot IPC socket path passed as -socket
  ENTMOOT_LOG   optional Entmoot log file (default: ~/.entmoot/log/entmootd.log)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --limit)
      limit="${2:?missing value for --limit}"
      shift 2
      ;;
    --log-minutes)
      log_minutes="${2:?missing value for --log-minutes}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "verify-mesh-node.sh: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

pilotctl_bin="${PILOTCTL:-pilotctl}"
entmootd_bin="${ENTMOOTD:-entmootd}"
entmoot_log="${ENTMOOT_LOG:-$HOME/.entmoot/log/entmootd.log}"

pilot_base=("$pilotctl_bin")
if [[ -n "${PILOT_SOCKET:-}" ]]; then
  pilot_base+=("-socket" "$PILOT_SOCKET")
fi

echo "== pilot version =="
"${pilot_base[@]}" version 2>/dev/null || "$pilotctl_bin" version 2>/dev/null || true

echo
echo "== pilot peers =="
"${pilot_base[@]}" peers 2>&1 || true

echo
echo "== pilot info summary =="
"${pilot_base[@]}" info 2>&1 | sed -n '1,80p' || true

echo
echo "== entmoot version =="
"$entmootd_bin" version 2>/dev/null || true

echo
echo "== entmoot message count =="
query_cmd=("$entmootd_bin" query --limit "$limit")
if [[ -n "${GROUP:-}" ]]; then
  query_cmd+=("-group" "$GROUP")
fi
"${query_cmd[@]}" 2>/dev/null | wc -l | tr -d ' '

echo
echo "== entmoot recent transport/reconcile lines =="
if [[ -f "$entmoot_log" ]]; then
  tail -300 "$entmoot_log" | grep -E 'turn-endpoint|transport_ad|reconcile|range response|message ingested|pilot:|yamux|session' | tail -80 || true
else
  echo "log file not found: $entmoot_log"
fi

echo
echo "== log window =="
echo "last ${log_minutes} minutes requested; file-log mode shows recent tail only"
