#!/usr/bin/env bash
set -euo pipefail

timeout=45
interval=0.25

usage() {
  cat <<'USAGE'
Usage: wait-pilot-ready.sh [--timeout seconds] [--interval seconds]

Polls pilotctl info until the local pilot-daemon IPC is ready.

Environment:
  PILOTCTL      pilotctl binary path (default: pilotctl)
  PILOT_SOCKET  optional Pilot IPC socket path passed as -socket
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeout)
      timeout="${2:?missing value for --timeout}"
      shift 2
      ;;
    --interval)
      interval="${2:?missing value for --interval}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "wait-pilot-ready.sh: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

pilotctl_bin="${PILOTCTL:-pilotctl}"
start="$(date +%s)"
deadline=$((start + timeout))
last_err=""

while :; do
  cmd=("$pilotctl_bin")
  if [[ -n "${PILOT_SOCKET:-}" ]]; then
    cmd+=("-socket" "$PILOT_SOCKET")
  fi
  cmd+=("info")

  if output="$("${cmd[@]}" 2>&1)"; then
    printf '%s\n' "$output"
    exit 0
  fi
  last_err="$output"

  now="$(date +%s)"
  if (( now >= deadline )); then
    echo "wait-pilot-ready.sh: pilot not ready after ${timeout}s" >&2
    if [[ -n "$last_err" ]]; then
      echo "last error: $last_err" >&2
    fi
    exit 1
  fi

  sleep "$interval"
done
