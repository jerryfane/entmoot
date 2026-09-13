#!/usr/bin/env bash
set -euo pipefail

limit=1000
log_minutes=10

usage() {
  cat <<'USAGE'
Usage: verify-mesh-node.sh [--limit n] [--log-minutes n]

Prints a local Entmoot libp2p health snapshot.

Environment:
  ENTMOOTD      entmootd binary path (default: entmootd)
  ENTMOOT_DATA  Entmoot data directory (default: ~/.entmoot)
  GROUP         optional Entmoot group id for query
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

entmootd_bin="${ENTMOOTD:-entmootd}"
entmoot_data="${ENTMOOT_DATA:-$HOME/.entmoot}"
entmoot_log="${ENTMOOT_LOG:-$entmoot_data/log/entmootd.log}"
base=("$entmootd_bin" -data "$entmoot_data")

echo "== entmoot version =="
"$entmootd_bin" version

echo
echo "== entmoot identity and libp2p endpoint =="
"${base[@]}" info --json

echo
echo "== entmoot message count =="
query_cmd=("${base[@]}" query --limit "$limit")
if [[ -n "${GROUP:-}" ]]; then
  query_cmd+=("-group" "$GROUP")
fi
"${query_cmd[@]}" --json | wc -l | tr -d ' '

echo
echo "== recent libp2p and reconciliation lines =="
if [[ -f "$entmoot_log" ]]; then
  tail -300 "$entmoot_log" | grep -E 'libp2p|reconcile|history|enroll|roster|message ingested' | tail -80 || true
else
  echo "log file not found: $entmoot_log"
fi

echo
echo "== log window =="
echo "last ${log_minutes} minutes requested; file-log mode shows recent tail only"
