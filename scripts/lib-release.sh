#!/usr/bin/env bash

log() {
  printf '%s\n' "$*"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

http_status() {
  local url="$1"
  curl -sS -o /dev/null -w '%{http_code}' --max-time "${HTTP_TIMEOUT:-10}" "$url"
}

expect_http_status() {
  local url="$1"
  local expected="$2"
  local label="$3"
  local status

  status="$(http_status "$url")" || die "$label request failed: $url"
  if [[ "$status" != "$expected" ]]; then
    die "$label returned HTTP $status, want $expected: $url"
  fi
  log "$label: HTTP $status"
}

systemd_available() {
  command -v systemctl >/dev/null 2>&1 && [[ -d /run/systemd/system ]]
}

service_is_active() {
  local service="$1"
  systemctl is-active --quiet "$service"
}

restart_service() {
  local service="$1"
  log "restarting $service"
  systemctl restart "$service"
  systemctl is-active --quiet "$service" || die "$service is not active after restart"
}
