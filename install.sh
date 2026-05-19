#!/bin/sh
# Entmoot installer.
# Usage:
#   Install:    curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh
#   Uninstall:  curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh -s uninstall
#
# Behaviour:
#   1. If a GitHub Release tarball exists for this OS/arch, download it.
#   2. Otherwise, fall back to cloning the repo and building from source
#      (requires Go and git).
#   3. Install entmootd to $HOME/.entmoot/bin and add that dir to PATH
#      via the user's shell rc file.
#
# Does NOT install Pilot Protocol. Pilot is a prerequisite; install the
# matching patched fork with:
#   curl -fsSL https://raw.githubusercontent.com/jerryfane/pilotprotocol/main/install.sh | sh
# Installing the upstream pilotprotocol.network version will work wire-wise
# but lacks the reliability patches these Entmoot releases rely on.

set -eu

REPO="jerryfane/entmoot"
PILOT_TAG="v1.7.2"
INSTALL_DIR="${ENTMOOT_HOME:-$HOME/.entmoot}"
BIN_DIR="$INSTALL_DIR/bin"
PILOT_DIR="${PILOT_HOME:-$HOME/.pilot}"
if [ -z "${PILOT_HOME:-}" ] && [ "$INSTALL_DIR" = "/data/.entmoot" ]; then
    PILOT_DIR="/data/.pilot"
fi
PILOT_BIN_DIR="${PILOT_BIN_DIR:-$PILOT_DIR/bin}"
PILOT_SOCKET_DEFAULT="/tmp/pilot.sock"
if [ -n "${PILOT_HOME:-}" ] || [ "$INSTALL_DIR" = "/data/.entmoot" ]; then
    PILOT_SOCKET_DEFAULT="$PILOT_DIR/pilot.sock"
fi
HELPER_MARKER="# entmoot-runtime-helper: $INSTALL_DIR"

remove_owned_helper() {
    path="$1"
    if [ ! -f "$path" ]; then
        return 0
    fi
    if grep -Fqx "$HELPER_MARKER" "$path" 2>/dev/null || grep -Fq "$INSTALL_DIR/runtime.env" "$path" 2>/dev/null; then
        rm -f "$path"
        echo "  Removed $path"
    fi
}

# --- Uninstall -----------------------------------------------------------

if [ "${1:-}" = "uninstall" ]; then
    echo ""
    echo "  Uninstalling Entmoot..."
    rm -rf "$INSTALL_DIR"
    echo "  Removed $INSTALL_DIR"
    remove_owned_helper "$PILOT_DIR/pilot"
    remove_owned_helper "$PILOT_DIR/start-entmoot-stack.sh"
    echo "  (You may want to remove Entmoot's PATH export from your shell rc file.)"
    echo ""
    exit 0
fi

# --- Detect platform ----------------------------------------------------

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
    arm64)   ARCH="arm64" ;;
    *)       echo "Error: unsupported architecture: $ARCH"; exit 1 ;;
esac

case "$OS" in
    linux|darwin) ;;
    *) echo "Error: unsupported OS: $OS"; exit 1 ;;
esac

echo ""
echo "  Entmoot installer"
echo "  Platform: ${OS}/${ARCH}"
echo "  Target:   ${BIN_DIR}"
echo ""

# --- Working directory --------------------------------------------------

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# --- Use a caller-provided binary ----------------------------------------

TAG=""
LOCAL_BINARY=0
if [ -n "${ENTMOOTD_LOCAL_BIN:-}" ]; then
    if [ ! -f "$ENTMOOTD_LOCAL_BIN" ]; then
        echo "  Error: ENTMOOTD_LOCAL_BIN does not exist: $ENTMOOTD_LOCAL_BIN"
        exit 1
    fi
    echo "  Using local entmootd binary: $ENTMOOTD_LOCAL_BIN"
    cp "$ENTMOOTD_LOCAL_BIN" "$TMPDIR/entmootd"
    chmod 755 "$TMPDIR/entmootd"
    LOCAL_BINARY=1
fi

# --- Try latest GitHub release ------------------------------------------

if [ "$LOCAL_BINARY" -eq 0 ] && [ -z "$TAG" ] && command -v curl >/dev/null 2>&1; then
    TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null \
          | grep '"tag_name"' | head -1 | cut -d'"' -f4 || true)
fi

if [ "$LOCAL_BINARY" -eq 0 ] && [ -n "$TAG" ]; then
    ARCHIVE="entmoot-${OS}-${ARCH}.tar.gz"
    URL="https://github.com/${REPO}/releases/download/${TAG}/${ARCHIVE}"
    echo "  Downloading ${TAG}..."
    if curl -fsSL "$URL" -o "$TMPDIR/$ARCHIVE" 2>/dev/null; then
        tar -xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR"
    else
        echo "  No prebuilt release for ${OS}/${ARCH}; falling back to source build."
        TAG=""
    fi
fi

# --- Fallback: build from source ----------------------------------------

if [ "$LOCAL_BINARY" -eq 0 ] && [ -z "$TAG" ]; then
    echo "  Building from source..."
    command -v go >/dev/null 2>&1 || {
        echo "  Error: Go is required to build from source."
        echo "  Install Go from https://go.dev/dl/ and retry."
        exit 1
    }
    command -v git >/dev/null 2>&1 || {
        echo "  Error: git is required to build from source."
        exit 1
    }
    echo "  Cloning entmoot..."
    git clone --depth 1 "https://github.com/${REPO}.git" "$TMPDIR/src" >/dev/null 2>&1
    echo "  Cloning Pilot fork (jerryfane/pilotprotocol main)..."
    git clone --depth 1 --branch main \
        https://github.com/jerryfane/pilotprotocol.git \
        "$TMPDIR/src/repos/pilotprotocol" >/dev/null 2>&1
    echo "  Building entmootd..."
    (cd "$TMPDIR/src/src" && CGO_ENABLED=0 go build -o "$TMPDIR/entmootd" ./cmd/entmootd)
fi

# --- Install -------------------------------------------------------------

mkdir -p "$BIN_DIR"
tmp_entmootd="$BIN_DIR/.entmootd.tmp.$$"
cp "$TMPDIR/entmootd" "$tmp_entmootd"
chmod 755 "$tmp_entmootd"
mv -f "$tmp_entmootd" "$BIN_DIR/entmootd"
echo "  Installed: $BIN_DIR/entmootd"

# --- Agent runtime helpers ----------------------------------------------

shell_quote() {
    printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

write_runtime_default() {
    name="$1"
    value="$2"
    if [ -n "$value" ]; then
        printf 'if [ -z "${%s+x}" ]; then %s=%s; fi\n' "$name" "$name" "$(shell_quote "$value")" >> "$INSTALL_DIR/runtime.env"
    fi
}

cat > "$INSTALL_DIR/runtime.env" <<EOF
if [ -z "\${ENTMOOT_BIN+x}" ]; then ENTMOOT_BIN=$(shell_quote "$BIN_DIR/entmootd"); fi
if [ -z "\${ENTMOOT_DATA+x}" ]; then ENTMOOT_DATA=$(shell_quote "$INSTALL_DIR"); fi
if [ -z "\${ENTMOOT_IDENTITY+x}" ]; then ENTMOOT_IDENTITY=$(shell_quote "$INSTALL_DIR/identity.json"); fi
if [ -z "\${ENTMOOT_CONTROL_SOCKET+x}" ]; then ENTMOOT_CONTROL_SOCKET=$(shell_quote "$INSTALL_DIR/control.sock"); fi
if [ -z "\${PILOT_DIR+x}" ]; then PILOT_DIR=$(shell_quote "$PILOT_DIR"); fi
if [ -z "\${PILOT_BIN_DIR+x}" ]; then PILOT_BIN_DIR=$(shell_quote "$PILOT_BIN_DIR"); fi
if [ -z "\${PILOT_SOCKET+x}" ]; then PILOT_SOCKET=$(shell_quote "$PILOT_SOCKET_DEFAULT"); fi
if [ -z "\${ENTMOOT_HIDE_IP+x}" ]; then ENTMOOT_HIDE_IP=0; fi
if [ -z "\${ENTMOOT_START_TIMEOUT+x}" ]; then ENTMOOT_START_TIMEOUT=90; fi
EOF
write_runtime_default ENTMOOT_RUN_USER "${ENTMOOT_RUN_USER:-}"
write_runtime_default PILOT_DAEMON_BIN "${PILOT_DAEMON_BIN:-}"
write_runtime_default PILOTCTL_BIN "${PILOTCTL_BIN:-}"
write_runtime_default PILOT_REGISTRY "${PILOT_REGISTRY:-}"
write_runtime_default PILOT_BEACON "${PILOT_BEACON:-}"
write_runtime_default PILOT_HOSTNAME "${PILOT_HOSTNAME:-}"
write_runtime_default PILOT_EMAIL "${PILOT_EMAIL:-}"
write_runtime_default PILOT_TURN_PROVIDER "${PILOT_TURN_PROVIDER:-}"
write_runtime_default PILOT_CLOUDFLARE_TURN_CREDS_FILE "${PILOT_CLOUDFLARE_TURN_CREDS_FILE:-}"
write_runtime_default PILOT_RENDEZVOUS_URL "${PILOT_RENDEZVOUS_URL:-}"

cat > "$INSTALL_DIR/entmoot" <<EOF
#!/bin/sh
$HELPER_MARKER
set -eu

RUNTIME_ENV=\${ENTMOOT_RUNTIME_ENV:-$(shell_quote "$INSTALL_DIR/runtime.env")}
if [ -f "\$RUNTIME_ENV" ]; then
  # shellcheck disable=SC1090
  . "\$RUNTIME_ENV"
fi

ENTMOOT_BIN=\${ENTMOOT_BIN:-$(shell_quote "$BIN_DIR/entmootd")}
ENTMOOT_DATA=\${ENTMOOT_DATA:-$(shell_quote "$INSTALL_DIR")}
ENTMOOT_IDENTITY=\${ENTMOOT_IDENTITY:-$(shell_quote "$INSTALL_DIR/identity.json")}
PILOT_SOCKET=\${PILOT_SOCKET:-$(shell_quote "$PILOT_SOCKET_DEFAULT")}

set -- -socket "\$PILOT_SOCKET" -identity "\$ENTMOOT_IDENTITY" -data "\$ENTMOOT_DATA" "\$@"
case "\${ENTMOOT_HIDE_IP:-0}" in
  1|true|yes) set -- -hide-ip "\$@" ;;
esac

exec "\$ENTMOOT_BIN" "\$@"
EOF
chmod 755 "$INSTALL_DIR/entmoot"
ln -sf "../entmoot" "$BIN_DIR/entmoot"
echo "  Installed: $INSTALL_DIR/entmoot"

mkdir -p "$PILOT_DIR"
cat > "$PILOT_DIR/pilot" <<EOF
#!/bin/sh
$HELPER_MARKER
set -eu

RUNTIME_ENV=\${ENTMOOT_RUNTIME_ENV:-$(shell_quote "$INSTALL_DIR/runtime.env")}
if [ -f "\$RUNTIME_ENV" ]; then
  # shellcheck disable=SC1090
  . "\$RUNTIME_ENV"
fi

PILOT_BIN_DIR=\${PILOT_BIN_DIR:-$(shell_quote "$PILOT_BIN_DIR")}
PILOT_SOCKET=\${PILOT_SOCKET:-$(shell_quote "$PILOT_SOCKET_DEFAULT")}

resolve_pilot_binary() {
  name="\$1"
  override="\$2"
  shift 2
  fallback="\$PILOT_BIN_DIR/\$name"
  if [ -n "\$override" ]; then
    if [ -x "\$override" ]; then
      printf '%s\n' "\$override"
      return 0
    fi
    echo "\$name is not executable at \$override" >&2
    return 1
  fi
  if [ -x "\$fallback" ]; then
    printf '%s\n' "\$fallback"
    return 0
  fi
  if command -v "\$name" >/dev/null 2>&1; then
    command -v "\$name"
    return 0
  fi
  echo "\$name not found; install Pilot or set PILOT_BIN_DIR/PILOTCTL_BIN" >&2
  return 1
}

PILOTCTL_BIN=\$(resolve_pilot_binary pilotctl "\${PILOTCTL_BIN:-}")

PILOT_SOCKET="\$PILOT_SOCKET" exec "\$PILOTCTL_BIN" "\$@"
EOF
chmod 755 "$PILOT_DIR/pilot"

cat > "$PILOT_DIR/start-entmoot-stack.sh" <<EOF
#!/bin/sh
$HELPER_MARKER
set -eu

RUNTIME_ENV=\${ENTMOOT_RUNTIME_ENV:-$(shell_quote "$INSTALL_DIR/runtime.env")}
if [ -f "\$RUNTIME_ENV" ]; then
  # shellcheck disable=SC1090
  . "\$RUNTIME_ENV"
fi

ENTMOOT_DATA=\${ENTMOOT_DATA:-$(shell_quote "$INSTALL_DIR")}
ENTMOOT_BIN=\${ENTMOOT_BIN:-$(shell_quote "$BIN_DIR/entmootd")}
ENTMOOT_IDENTITY=\${ENTMOOT_IDENTITY:-$(shell_quote "$INSTALL_DIR/identity.json")}
PILOT_DIR=\${PILOT_DIR:-$(shell_quote "$PILOT_DIR")}
PILOT_BIN_DIR=\${PILOT_BIN_DIR:-$(shell_quote "$PILOT_BIN_DIR")}
PILOT_SOCKET=\${PILOT_SOCKET:-$(shell_quote "$PILOT_SOCKET_DEFAULT")}
TMP_PILOT_SOCKET=\${TMP_PILOT_SOCKET:-/tmp/pilot.sock}
RUN_DIR=\${ENTMOOT_RUN_DIR:-"\$ENTMOOT_DATA/run"}
PILOT_PIDFILE="\$RUN_DIR/pilot-daemon.pid"
ENTMOOT_PIDFILE="\$RUN_DIR/entmootd.pid"
ENTMOOT_CONTROL_SOCKET=\${ENTMOOT_CONTROL_SOCKET:-"\$ENTMOOT_DATA/control.sock"}
ENTMOOT_START_TIMEOUT=\${ENTMOOT_START_TIMEOUT:-90}
case "\$ENTMOOT_START_TIMEOUT" in
  ''|*[!0-9]*)
    echo "invalid ENTMOOT_START_TIMEOUT=\$ENTMOOT_START_TIMEOUT; using 90" >&2
    ENTMOOT_START_TIMEOUT=90
    ;;
esac

quote() {
  printf "'%s'" "\$(printf '%s' "\$1" | sed "s/'/'\\\\''/g")"
}

has_proc_arg() {
  pid="\$1"
  want="\$2"
  [ -r "/proc/\$pid/cmdline" ] || return 1
  tr '\000' '\n' < "/proc/\$pid/cmdline" | grep -Fx -- "\$want" >/dev/null 2>&1
}

pid_matches_stack() {
  kind="\$1"
  pid="\$2"
  [ -n "\$pid" ] || return 1
  kill -0 "\$pid" 2>/dev/null || return 1
  [ -r "/proc/\$pid/cmdline" ] || return 1
  case "\$kind" in
    pilot)
      has_proc_arg "\$pid" "-socket" && has_proc_arg "\$pid" "\$PILOT_SOCKET"
      ;;
    entmoot)
      if has_proc_arg "\$pid" "-data" && has_proc_arg "\$pid" "\$ENTMOOT_DATA"; then
        return 0
      fi
      has_proc_arg "\$pid" "\$ENTMOOT_BIN" && has_proc_arg "\$pid" "serve" && ! has_proc_arg "\$pid" "esp"
      ;;
    agent-watcher)
      has_proc_arg "\$pid" "-data" && has_proc_arg "\$pid" "\$ENTMOOT_DATA" && has_proc_arg "\$pid" "agent-commands" && has_proc_arg "\$pid" "watch"
      ;;
    *)
      return 1
      ;;
  esac
}

socket_path_exists() {
  path="\$1"
  [ -e "\$path" ] || [ -S "\$path" ] || [ -L "\$path" ]
}

pidfile_pid() {
  pidfile="\$1"
  if [ -f "\$pidfile" ]; then
    cat "\$pidfile" 2>/dev/null || true
  fi
}

pidfile_live_for_kind() {
  kind="\$1"
  pidfile="\$2"
  pid=\$(pidfile_pid "\$pidfile")
  pid_matches_stack "\$kind" "\$pid"
}

remove_stale_pidfile() {
  kind="\$1"
  pidfile="\$2"
  [ -f "\$pidfile" ] || return 1
  if pidfile_live_for_kind "\$kind" "\$pidfile"; then
    return 1
  fi
  rm -f "\$pidfile"
}

stop_pidfile() {
  kind="\$1"
  pidfile="\$2"
  pid=\$(pidfile_pid "\$pidfile")
  stopped=0
  if pid_matches_stack "\$kind" "\$pid"; then
    stopped=1
    kill -TERM "\$pid" 2>/dev/null || true
    sleep 2
    if pid_matches_stack "\$kind" "\$pid"; then
      kill -KILL "\$pid" 2>/dev/null || true
    fi
  fi
  rm -f "\$pidfile"
  [ "\$stopped" = "1" ]
}

stop_stack_processes() {
  kind="\$1"
  for proc in /proc/[0-9]*; do
    [ -d "\$proc" ] || continue
    pid="\${proc#/proc/}"
    if pid_matches_stack "\$kind" "\$pid"; then
      kill -TERM "\$pid" 2>/dev/null || true
    fi
  done
  sleep 2
  for proc in /proc/[0-9]*; do
    [ -d "\$proc" ] || continue
    pid="\${proc#/proc/}"
    if pid_matches_stack "\$kind" "\$pid"; then
      kill -KILL "\$pid" 2>/dev/null || true
    fi
  done
}

is_data_agent_layout() {
  [ "\$ENTMOOT_DATA" = "/data/.entmoot" ] && return 0
  [ "\$PILOT_DIR" = "/data/.pilot" ] && return 0
  case "\$PILOT_SOCKET" in
    /data/.pilot/*) return 0 ;;
  esac
  return 1
}

select_stack_run_user() {
  if [ -n "\${ENTMOOT_RUN_USER:-}" ]; then
    id "\$ENTMOOT_RUN_USER" >/dev/null 2>&1 || {
      echo "ENTMOOT_RUN_USER=\$ENTMOOT_RUN_USER does not exist" >&2
      return 1
    }
    if [ "\$(id -u "\$ENTMOOT_RUN_USER" 2>/dev/null || printf 0)" = "0" ]; then
      return 1
    fi
    printf '%s\n' "\$ENTMOOT_RUN_USER"
    return 0
  fi
  for candidate in node ubuntu; do
    if id "\$candidate" >/dev/null 2>&1; then
      printf '%s\n' "\$candidate"
      return 0
    fi
  done
  return 1
}

remove_compat_pilot_socket() {
  if [ "\$TMP_PILOT_SOCKET" = "\$PILOT_SOCKET" ]; then
    return 0
  fi
  if [ -L "\$TMP_PILOT_SOCKET" ] && [ "\$(readlink "\$TMP_PILOT_SOCKET" 2>/dev/null || true)" = "\$PILOT_SOCKET" ]; then
    rm -f "\$TMP_PILOT_SOCKET"
  fi
}

install_compat_pilot_socket() {
  if [ "\$TMP_PILOT_SOCKET" = "\$PILOT_SOCKET" ]; then
    return 0
  fi
  if [ -e "\$TMP_PILOT_SOCKET" ] || [ -L "\$TMP_PILOT_SOCKET" ]; then
    if [ -L "\$TMP_PILOT_SOCKET" ] && [ "\$(readlink "\$TMP_PILOT_SOCKET" 2>/dev/null || true)" = "\$PILOT_SOCKET" ]; then
      ln -sf "\$PILOT_SOCKET" "\$TMP_PILOT_SOCKET"
    else
      echo "leaving existing \$TMP_PILOT_SOCKET in place; it is not this stack's compatibility symlink" >&2
    fi
  else
    ln -s "\$PILOT_SOCKET" "\$TMP_PILOT_SOCKET"
  fi
}

pilot_socket_live() {
  [ -S "\$PILOT_SOCKET" ] || return 1
  run_pilotctl info >/dev/null 2>&1
}

remove_stale_socket_if_dead() {
  path="\$1"
  probe="\$2"
  socket_path_exists "\$path" || return 1
  if "\$probe"; then
    return 1
  fi
  rm -f "\$path"
}

require_pilot_live() {
  if pilot_socket_live; then
    return 0
  fi
  echo "pilot socket did not appear at \$PILOT_SOCKET" >&2
  return 1
}

resolve_pilot_binary() {
  name="\$1"
  override="\$2"
  shift 2
  fallback="\$PILOT_BIN_DIR/\$name"
  if [ -n "\$override" ]; then
    if [ -x "\$override" ]; then
      printf '%s\n' "\$override"
      return 0
    fi
    echo "\$name is not executable at \$override" >&2
    return 1
  fi
  for candidate in "\$name" "\$@"; do
    [ -n "\$candidate" ] || continue
    fallback="\$PILOT_BIN_DIR/\$candidate"
    if [ -x "\$fallback" ]; then
      printf '%s\n' "\$fallback"
      return 0
    fi
  done
  for candidate in "\$name" "\$@"; do
    [ -n "\$candidate" ] || continue
    if command -v "\$candidate" >/dev/null 2>&1; then
      command -v "\$candidate"
      return 0
    fi
  done
  echo "\$name not found; install Pilot or set PILOT_BIN_DIR/PILOT_DAEMON_BIN/PILOTCTL_BIN" >&2
  return 1
}

run_pilotctl() {
  PILOT_SOCKET="\$PILOT_SOCKET" "\$PILOTCTL_BIN" "\$@"
}

entmoot_hide_ip_enabled() {
  case "\${ENTMOOT_HIDE_IP:-0}" in
    1|true|yes) return 0 ;;
    *) return 1 ;;
  esac
}

entmoot_pid_alive() {
  pid="\$1"
  [ -n "\$pid" ] && kill -0 "\$pid" 2>/dev/null
}

entmoot_socket_live() {
  [ -S "\$ENTMOOT_CONTROL_SOCKET" ] || return 1
  "\$ENTMOOT_BIN" -socket "\$PILOT_SOCKET" -identity "\$ENTMOOT_IDENTITY" -data "\$ENTMOOT_DATA" info 2>/dev/null | grep '"running":[[:space:]]*true' >/dev/null 2>&1
}

require_entmoot_live() {
  entmoot_socket_live
}

print_entmoot_start_failure() {
  echo "entmoot daemon did not become ready at \$ENTMOOT_CONTROL_SOCKET" >&2
  if [ -f "\$ENTMOOT_DATA/restart.log" ]; then
    echo "last entmoot log lines:" >&2
    tail -n 40 "\$ENTMOOT_DATA/restart.log" >&2 || true
  fi
}

stop_agent_command_watcher() {
  watch_pidfile="\$RUN_DIR/agent-commands-watch.pid"
  old_pid=""
  if [ -f "\$watch_pidfile" ]; then
    old_pid=\$(cat "\$watch_pidfile" 2>/dev/null || true)
  fi
  if pid_matches_stack agent-watcher "\$old_pid"; then
    kill -TERM "\$old_pid" 2>/dev/null || true
    sleep 1
    if pid_matches_stack agent-watcher "\$old_pid"; then
      kill -KILL "\$old_pid" 2>/dev/null || true
    fi
  fi
  stop_stack_processes agent-watcher || true
  rm -f "\$watch_pidfile"
}

runner_command_available() {
  candidate="\$1"
  [ -n "\$candidate" ] || return 1
  [ -x "\$candidate" ] || command -v "\$candidate" >/dev/null 2>&1
}

validate_agent_command_runner() {
  runner="\${ENTMOOT_AGENT_RUNNER:-}"
  [ -n "\$runner" ] || return 0
  if [ "\$runner" = "openclaw" ]; then
    openclaw_bin="\${OPENCLAW_BIN:-openclaw}"
    if ! runner_command_available "\$openclaw_bin"; then
      echo "ENTMOOT_AGENT_RUNNER=openclaw requires OPENCLAW_BIN to be executable or openclaw on PATH" >&2
      return 1
    fi
  elif ! runner_command_available "\$runner"; then
    echo "ENTMOOT_AGENT_RUNNER=\$runner is not executable or on PATH" >&2
    return 1
  fi
}

start_agent_command_watcher() {
  validate_agent_command_runner || return 1
  stop_agent_command_watcher
  runner="\${ENTMOOT_AGENT_RUNNER:-}"
  [ -n "\$runner" ] || return 0
  interval="\${ENTMOOT_AGENT_WATCH_INTERVAL:-10s}"
  watch_pidfile="\$RUN_DIR/agent-commands-watch.pid"
  nohup "\$ENTMOOT_BIN" -socket "\$PILOT_SOCKET" -identity "\$ENTMOOT_IDENTITY" -data "\$ENTMOOT_DATA" agent-commands watch -interval "\$interval" -runner "\$runner" >> "\$ENTMOOT_DATA/agent-commands-watch.log" 2>&1 &
  echo "\$!" > "\$watch_pidfile"
}

if [ "\$(id -u)" = "0" ] && [ -d /data ] && is_data_agent_layout; then
  if STACK_RUN_USER=\$(select_stack_run_user); then
    mkdir -p "\$RUN_DIR"
    stop_agent_command_watcher || true
    stop_pidfile entmoot "\$ENTMOOT_PIDFILE" || true
    stop_pidfile pilot "\$PILOT_PIDFILE" || true
    stop_stack_processes entmoot || true
    stop_stack_processes pilot || true
    remove_compat_pilot_socket || true
    STACK_RUN_GROUP=\$(id -gn "\$STACK_RUN_USER" 2>/dev/null || printf '%s' "\$STACK_RUN_USER")
    chown -R "\$STACK_RUN_USER:\$STACK_RUN_GROUP" "\$PILOT_DIR" "\$ENTMOOT_DATA" "\$RUN_DIR"
    envs="ENTMOOT_RUNTIME_ENV=\$(quote "\$RUNTIME_ENV")"
    for name in ENTMOOT_BIN ENTMOOT_DATA ENTMOOT_IDENTITY ENTMOOT_CONTROL_SOCKET ENTMOOT_HIDE_IP ENTMOOT_START_TIMEOUT ENTMOOT_RUN_DIR ENTMOOT_RUN_USER ENTMOOT_AGENT_INSTRUCTIONS ENTMOOT_AGENT_RUNNER ENTMOOT_AGENT_WATCH_INTERVAL ENTMOOT_OPENCLAW_AGENT ENTMOOT_OPENCLAW_SESSION_ID ENTMOOT_OPENCLAW_TO OPENCLAW_AGENT_ID OPENCLAW_SESSION_ID OPENCLAW_TO OPENCLAW_BIN PILOT_DIR PILOT_BIN_DIR PILOT_SOCKET TMP_PILOT_SOCKET PILOT_DAEMON_BIN PILOTCTL_BIN PILOT_REGISTRY PILOT_BEACON PILOT_HOSTNAME PILOT_EMAIL PILOT_TURN_PROVIDER PILOT_CLOUDFLARE_TURN_CREDS_FILE PILOT_RENDEZVOUS_URL; do
      eval "value=\\\${\$name-}"
      if [ -n "\$value" ]; then
        envs="\$envs \$name=\$(quote "\$value")"
      fi
    done
    exec su "\$STACK_RUN_USER" -c "\$envs sh \$(quote "\$0")"
  elif [ -n "\${ENTMOOT_RUN_USER:-}" ]; then
    exit 1
  fi
fi

PILOT_DAEMON_BIN=\$(resolve_pilot_binary pilot-daemon "\${PILOT_DAEMON_BIN:-}" daemon)
PILOTCTL_BIN=\$(resolve_pilot_binary pilotctl "\${PILOTCTL_BIN:-}")

mkdir -p "\$RUN_DIR"
validate_agent_command_runner || exit 1
stop_agent_command_watcher || true
stop_pidfile entmoot "\$ENTMOOT_PIDFILE" || true
stop_stack_processes entmoot || true
if socket_path_exists "\$ENTMOOT_CONTROL_SOCKET"; then
  if entmoot_socket_live; then
    echo "entmoot control socket is already live at \$ENTMOOT_CONTROL_SOCKET; refusing to start an unmanaged duplicate" >&2
    exit 1
  fi
  remove_stale_socket_if_dead "\$ENTMOOT_CONTROL_SOCKET" entmoot_socket_live || true
fi
pilot_stopped=0
if stop_pidfile pilot "\$PILOT_PIDFILE"; then
  pilot_stopped=1
fi
if [ "\$pilot_stopped" = "1" ] || ! socket_path_exists "\$PILOT_SOCKET"; then
  rm -f "\$PILOT_SOCKET"
fi
remove_stale_socket_if_dead "\$PILOT_SOCKET" pilot_socket_live || true
remove_compat_pilot_socket

if ! pilot_socket_live; then
  set -- -socket "\$PILOT_SOCKET" -identity "\$PILOT_DIR/identity.json" -email "\${PILOT_EMAIL:-agent@example.com}" -listen :0
  if [ -n "\${PILOT_REGISTRY:-}" ]; then
    set -- "\$@" -registry "\$PILOT_REGISTRY"
  fi
  if [ -n "\${PILOT_BEACON:-}" ]; then
    set -- "\$@" -beacon "\$PILOT_BEACON"
  fi
  if [ -n "\${PILOT_HOSTNAME:-}" ]; then
    set -- "\$@" -hostname "\$PILOT_HOSTNAME"
  fi
  if entmoot_hide_ip_enabled; then
    set -- "\$@" -no-registry-endpoint -outbound-turn-only
    if [ -n "\${PILOT_TURN_PROVIDER:-}" ]; then
      set -- "\$@" -turn-provider "\$PILOT_TURN_PROVIDER"
    fi
    if [ -n "\${PILOT_CLOUDFLARE_TURN_CREDS_FILE:-}" ]; then
      set -- "\$@" -cloudflare-turn-creds-file "\$PILOT_CLOUDFLARE_TURN_CREDS_FILE"
    fi
    if [ -n "\${PILOT_RENDEZVOUS_URL:-}" ]; then
      set -- "\$@" -rendezvous-url "\$PILOT_RENDEZVOUS_URL"
    fi
  fi

  nohup "\$PILOT_DAEMON_BIN" "\$@" >> "\$PILOT_DIR/restart.log" 2>&1 &
  echo "\$!" > "\$PILOT_PIDFILE"
fi

for _ in 1 2 3 4 5 6 7 8 9 10 11 12; do
  pilot_socket_live && break
  sleep 1
done

require_pilot_live || exit 1
install_compat_pilot_socket

set -- -socket "\$PILOT_SOCKET" -identity "\$ENTMOOT_IDENTITY" -data "\$ENTMOOT_DATA" serve
if entmoot_hide_ip_enabled; then
  set -- -hide-ip "\$@"
fi

nohup "\$ENTMOOT_BIN" "\$@" >> "\$ENTMOOT_DATA/restart.log" 2>&1 &
entmoot_pid="\$!"
elapsed=0
while [ "\$elapsed" -lt "\$ENTMOOT_START_TIMEOUT" ]; do
  if ! entmoot_pid_alive "\$entmoot_pid"; then
    rm -f "\$ENTMOOT_PIDFILE"
    print_entmoot_start_failure
    exit 1
  fi
  if require_entmoot_live; then
    echo "\$entmoot_pid" > "\$ENTMOOT_PIDFILE"
    if start_agent_command_watcher; then
      exit 0
    fi
    kill -TERM "\$entmoot_pid" 2>/dev/null || true
    rm -f "\$ENTMOOT_PIDFILE"
    exit 1
  fi
  sleep 1
  elapsed=\$((elapsed + 1))
done

if entmoot_pid_alive "\$entmoot_pid"; then
  echo "\$entmoot_pid" > "\$ENTMOOT_PIDFILE"
  echo "entmoot daemon is still not ready after \$ENTMOOT_START_TIMEOUT seconds; leaving process \$entmoot_pid running" >&2
  exit 1
fi
rm -f "\$ENTMOOT_PIDFILE"
print_entmoot_start_failure
exit 1
EOF
chmod 755 "$PILOT_DIR/start-entmoot-stack.sh"

# --- PATH setup ----------------------------------------------------------

case ":$PATH:" in
    *":${BIN_DIR}:"*)
        ;;
    *)
        SHELL_NAME=$(basename "${SHELL:-/bin/sh}" 2>/dev/null || echo "sh")
        case "$SHELL_NAME" in
            zsh)  RC="$HOME/.zshrc" ;;
            bash) RC="$HOME/.bashrc" ;;
            *)    RC="$HOME/.profile" ;;
        esac
        if [ ! -f "$RC" ] || ! grep -q "${BIN_DIR}" "$RC" 2>/dev/null; then
            {
                echo ""
                echo "# Entmoot"
                echo "export PATH=\"${BIN_DIR}:\$PATH\""
            } >> "$RC"
            echo "  Added ${BIN_DIR} to PATH in ${RC}"
        fi
        ;;
esac

# --- Done ----------------------------------------------------------------

echo ""
echo "Installed:"
echo "  entmootd  ${BIN_DIR}/entmootd"
echo ""
echo "Next steps:"
echo ""
echo "  # activate the new PATH in this shell:"
echo "  export PATH=\"${BIN_DIR}:\$PATH\""
echo ""
echo "  # install the patched Pilot Protocol (prerequisite, separate):"
echo "  curl -fsSL https://raw.githubusercontent.com/jerryfane/pilotprotocol/main/install.sh | sh"
echo ""
echo "  # verify:"
echo "  entmootd info"
echo ""
