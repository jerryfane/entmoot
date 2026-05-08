#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib-release.sh
source "$script_dir/lib-release.sh"

service="${ESP_SERVICE:-entmoot-esp.service}"
local_url="${ESP_LOCAL_URL:-http://127.0.0.1:8087}"
public_url="${ESP_PUBLIC_URL:-https://esp.entmoot.xyz}"
session_status="${ESP_SESSION_STATUS:-401}"
check_public=1

usage() {
  cat <<'USAGE'
Usage: verify-esp-service.sh [--service name] [--local-url url] [--public-url url] [--no-public] [--session-status code]

Verifies that the Entmoot ESP bridge is reachable locally and, by default,
through the public reverse proxy.

Environment:
  ESP_SERVICE         systemd service name (default: entmoot-esp.service)
  ESP_LOCAL_URL       local ESP base URL (default: http://127.0.0.1:8087)
  ESP_PUBLIC_URL      public ESP base URL (default: https://esp.entmoot.xyz)
  ESP_SESSION_STATUS  expected unauthenticated /v1/session status (default: 401)
  HTTP_TIMEOUT        curl timeout in seconds (default: 10)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service)
      service="${2:?missing value for --service}"
      shift 2
      ;;
    --local-url)
      local_url="${2:?missing value for --local-url}"
      shift 2
      ;;
    --public-url)
      public_url="${2:?missing value for --public-url}"
      check_public=1
      shift 2
      ;;
    --no-public)
      check_public=0
      shift
      ;;
    --session-status)
      session_status="${2:?missing value for --session-status}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "verify-esp-service.sh: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

require_cmd curl

if systemd_available; then
  service_is_active "$service" || die "$service is not active"
  log "$service: active"
else
  log "systemd not detected; skipping service state check"
fi

expect_http_status "${local_url%/}/healthz" "200" "local ESP health"

if (( check_public )); then
  expect_http_status "${public_url%/}/healthz" "200" "public ESP health"
  expect_http_status "${public_url%/}/v1/session" "$session_status" "public ESP auth boundary"
fi
