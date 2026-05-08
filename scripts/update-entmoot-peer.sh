#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib-release.sh
source "$script_dir/lib-release.sh"

tag=""
install_dir="${ENTMOOT_INSTALL_DIR:-$HOME/.entmoot/bin}"
entmootd_bin="${ENTMOOTD:-}"
serve_service="${ENTMOOT_SERVE_SERVICE:-}"
serve_restart_cmd="${ENTMOOT_SERVE_RESTART_CMD:-}"
esp_service="${ESP_SERVICE:-entmoot-esp.service}"
esp_url="${ESP_PUBLIC_URL:-https://esp.entmoot.xyz}"
restart_esp=0
verify_esp=0
stop_timeout="${ENTMOOT_SERVE_STOP_TIMEOUT:-15}"

usage() {
  cat <<'USAGE'
Usage: update-entmoot-peer.sh --tag vX.Y.Z [options]

Updates the Entmoot binary on a peer and restarts only explicitly named
services. It never broad-kills entmootd processes.

Options:
  --tag vX.Y.Z             release tag to install
  --install-dir dir        directory containing entmootd (default: ~/.entmoot/bin)
  --entmootd path          entmootd binary to invoke (default: install-dir/entmootd)
  --serve-service name     systemd service for the main entmootd serve daemon
  --serve-restart-cmd cmd  command to start unmanaged entmootd serve after update
  --stop-timeout seconds   wait for old unmanaged serve PIDs to exit (default: 15)
  --restart-esp            restart the ESP service after updating
  --verify-esp             verify ESP local/public health after updating
  --esp-service name       ESP systemd service (default: entmoot-esp.service)
  --esp-url url            public ESP base URL (default: https://esp.entmoot.xyz)

Environment:
  ENTMOOT_SERVE_RESTART_CMD  same as --serve-restart-cmd
  ESP_LOCAL_URL              local ESP base URL for verify-esp-service.sh
  ESP_SESSION_STATUS         expected unauthenticated /v1/session status
  ENTMOOT_SERVE_STOP_TIMEOUT same as --stop-timeout

Use either --serve-service or --serve-restart-cmd, not both.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      tag="${2:?missing value for --tag}"
      shift 2
      ;;
    --install-dir)
      install_dir="${2:?missing value for --install-dir}"
      shift 2
      ;;
    --entmootd)
      entmootd_bin="${2:?missing value for --entmootd}"
      shift 2
      ;;
    --serve-service)
      serve_service="${2:?missing value for --serve-service}"
      shift 2
      ;;
    --serve-restart-cmd)
      serve_restart_cmd="${2:?missing value for --serve-restart-cmd}"
      shift 2
      ;;
    --stop-timeout)
      stop_timeout="${2:?missing value for --stop-timeout}"
      shift 2
      ;;
    --restart-esp)
      restart_esp=1
      shift
      ;;
    --verify-esp)
      verify_esp=1
      shift
      ;;
    --esp-service)
      esp_service="${2:?missing value for --esp-service}"
      shift 2
      ;;
    --esp-url)
      esp_url="${2:?missing value for --esp-url}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "update-entmoot-peer.sh: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ -n "$tag" ]] || die "--tag is required"
[[ -n "$entmootd_bin" ]] || entmootd_bin="${install_dir%/}/entmootd"
if [[ -n "$serve_service" && -n "$serve_restart_cmd" ]]; then
  die "--serve-service and --serve-restart-cmd/ENTMOOT_SERVE_RESTART_CMD are mutually exclusive"
fi

require_cmd "$entmootd_bin"
entmootd_bin="$(command -v "$entmootd_bin")"

canonical_path() {
  local path="$1"
  local dir
  local base
  dir="$(dirname "$path")"
  base="$(basename "$path")"
  (cd "$dir" && printf '%s/%s\n' "$PWD" "$base")
}

entmoot_top_level_subcommand() {
  local -a args=("$@")
  local i
  local arg
  for (( i = 1; i < ${#args[@]}; i++ )); do
    arg="${args[$i]}"
    if [[ "$arg" == "--" ]]; then
      if (( i + 1 < ${#args[@]} )); then
        printf '%s\n' "${args[$((i + 1))]}"
      fi
      return
    fi
    if [[ "$arg" == -* ]]; then
      case "${arg%%=*}" in
        -socket|--socket|-identity|--identity|-data|--data|-listen-port|--listen-port|-log-level|--log-level|-pilot-wait-timeout|--pilot-wait-timeout|-pilot-wait-base-delay|--pilot-wait-base-delay|-pilot-wait-max-delay|--pilot-wait-max-delay)
          if [[ "$arg" != *=* ]]; then
            (( i++ ))
          fi
          ;;
      esac
      continue
    fi
    printf '%s\n' "$arg"
    return
  done
}

pid_is_entmoot_serve() {
  local pid="$1"
  local expected="$2"
  local -a args=()
  local arg
  local exe
  local exe_dir
  local exe_base
  while IFS= read -r arg; do
    [[ -n "$arg" ]] && args+=("$arg")
  done < <(tr '\0' '\n' <"/proc/$pid/cmdline" 2>/dev/null) || return 1
  [[ "${#args[@]}" -gt 0 ]] || return 1
  case "$(entmoot_top_level_subcommand "${args[@]}")" in
    serve|join) ;;
    *) return 1 ;;
  esac
  exe="$(readlink "/proc/$pid/exe" 2>/dev/null)" || return 1
  exe="${exe% (deleted)}"
  exe_dir="$(dirname "$exe")"
  exe_base="$(basename "$exe")"
  [[ "$exe_dir" == "$(dirname "$expected")" ]] || return 1
  [[ "$exe_base" == "entmootd" || "$exe_base" == "entmootd.bak" ]] || return 1
}

find_unmanaged_serve_pids() {
  local expected="$1"
  local entry
  local pid
  [[ -d /proc ]] || return 0
  for entry in /proc/[0-9]*; do
    pid="${entry##*/}"
    if [[ "$pid" != "$$" ]] && pid_is_entmoot_serve "$pid" "$expected"; then
      printf '%s\n' "$pid"
    fi
  done
}

wait_for_old_serve_pids() {
  local deadline=$((SECONDS + stop_timeout))
  local pid
  local still_running
  local expected="$1"
  shift
  if (($# == 0)); then
    return
  fi
  while :; do
    still_running=0
    for pid in "$@"; do
      if [[ -d "/proc/$pid" ]] && pid_is_entmoot_serve "$pid" "$expected"; then
        still_running=1
        break
      fi
    done
    if (( ! still_running )); then
      return
    fi
    if (( SECONDS >= deadline )); then
      die "old entmootd serve process did not exit within ${stop_timeout}s"
    fi
    sleep 0.25
  done
}

stop_unmanaged_serve_pids() {
  local expected="$1"
  shift
  local pid
  if (($# == 0)); then
    return
  fi
  for pid in "$@"; do
    if [[ -d "/proc/$pid" ]] && pid_is_entmoot_serve "$pid" "$expected"; then
      log "stopping unmanaged entmootd serve pid $pid"
      kill -TERM "$pid"
    fi
  done
  wait_for_old_serve_pids "$expected" "$@"
}

entmootd_path="$(canonical_path "$entmootd_bin")"
serve_pids=()
if [[ -n "$serve_restart_cmd" ]]; then
  [[ -d /proc ]] || die "--serve-restart-cmd requires /proc so stopped serve PIDs can be verified"
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && serve_pids+=("$pid")
  done < <(find_unmanaged_serve_pids "$entmootd_path")
fi

log "updating Entmoot to $tag via $entmootd_bin"
update_cmd=("$entmootd_bin" update --json --tag "$tag" --install-dir "$install_dir")
if [[ -n "$serve_restart_cmd" ]]; then
  update_cmd+=(--restart)
fi
set +e
update_output="$("${update_cmd[@]}" 2>&1)"
update_status=$?
set -e
printf '%s\n' "$update_output"
if (( update_status != 0 )); then
  exit "$update_status"
fi
updated=0
if grep -Eq '"updated"[[:space:]]*:[[:space:]]*true' <<<"$update_output"; then
  updated=1
fi

if (( ! updated )); then
  log "no update applied"
fi

if [[ -n "$serve_service" ]]; then
  systemd_available || die "--serve-service requires systemd"
  restart_service "$serve_service"
elif [[ -n "$serve_restart_cmd" ]]; then
  if (( updated )); then
    wait_for_old_serve_pids "$entmootd_path" "${serve_pids[@]}"
  else
    stop_unmanaged_serve_pids "$entmootd_path" "${serve_pids[@]}"
  fi
  log "running explicit Entmoot serve restart command"
  bash -c "$serve_restart_cmd"
else
  log "no Entmoot serve restart requested; leaving serve lifecycle unchanged"
fi

if (( restart_esp )); then
  systemd_available || die "ESP service restart requires systemd"
  restart_service "$esp_service"
fi

if (( verify_esp )); then
  "$script_dir/verify-esp-service.sh" --service "$esp_service" --public-url "$esp_url"
fi
