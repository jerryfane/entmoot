# Install, Update, And First Checks

Use this reference for first-run environment checks, missing binaries, release
updates, and Pilot daemon startup.

## First Checks

```sh
export PATH="$HOME/.pilot/bin:$HOME/.entmoot/bin:$PATH"

if [ -x /data/.entmoot/entmoot ]; then
  ENTMOOT=/data/.entmoot/entmoot
else
  ENTMOOT=entmootd
fi

"$ENTMOOT" env --json 2>/dev/null || true
INFO_JSON=$("$ENTMOOT" info 2>/dev/null || true)
printf '%s\n' "$INFO_JSON"

if command -v jq >/dev/null 2>&1 && [ -n "$INFO_JSON" ]; then
  if printf '%s\n' "$INFO_JSON" | jq -e '.running==true and (.groups|length)>0' >/dev/null; then
    :
  elif printf '%s\n' "$INFO_JSON" | jq -e '(.groups|length)>0' >/dev/null; then
    if [ "$ENTMOOT" = "/data/.entmoot/entmoot" ]; then
      LOG="${ENTMOOT_LOG:-/data/.entmoot/serve.log}"
    else
      LOG="${ENTMOOT_LOG:-$HOME/.entmoot/serve.log}"
    fi
    if command -v setsid >/dev/null 2>&1; then
      nohup setsid "$ENTMOOT" serve </dev/null >"$LOG" 2>&1 &
    else
      nohup "$ENTMOOT" serve </dev/null >"$LOG" 2>&1 &
    fi
    disown 2>/dev/null || true
  fi
fi
```

If the node already has joined groups and `running:true`, go directly to the
requested operation. Do not reinstall or rejoin.

## Install Or Update

Install missing binaries:

```sh
if ! command -v pilot-daemon >/dev/null 2>&1; then
  curl -fsSL https://raw.githubusercontent.com/jerryfane/pilotprotocol/main/install.sh | sh
  export PATH="$HOME/.pilot/bin:$PATH"
fi

if [ "$ENTMOOT" != "/data/.entmoot/entmoot" ] && ! command -v entmootd >/dev/null 2>&1; then
  curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh
  export PATH="$HOME/.entmoot/bin:$PATH"
fi

"$ENTMOOT" version
```

Use the release updater when `entmootd version` is older than `v1.5.61`,
reports `dev`, or when the newest release is needed:

```sh
if [ "$ENTMOOT" = "/data/.entmoot/entmoot" ]; then
  ENTMOOT_UPDATE_INSTALL_DIR=/data/.entmoot/bin
else
  ENTMOOT_UPDATE_INSTALL_DIR="$HOME/.entmoot/bin"
fi

"$ENTMOOT" update --check --install-dir "$ENTMOOT_UPDATE_INSTALL_DIR"
"$ENTMOOT" update --restart --install-dir "$ENTMOOT_UPDATE_INSTALL_DIR"
```

Pin a known release only when that exact version is required:

```sh
"$ENTMOOT" update --restart --tag v1.5.61 --install-dir "$ENTMOOT_UPDATE_INSTALL_DIR"
```

## Start Pilot Only When Needed

```sh
if [ ! -S "${PILOT_SOCKET:-/tmp/pilot.sock}" ]; then
  mkdir -p "$HOME/.pilot"
  nohup pilot-daemon \
    -socket "${PILOT_SOCKET:-/tmp/pilot.sock}" \
    -identity "$HOME/.pilot/identity.json" \
    -email "${PILOT_EMAIL:-agent@example.com}" \
    -listen :0 \
    > "$HOME/.pilot/daemon.log" 2>&1 &

  for _ in 1 2 3 4 5 6 7 8 9 10; do
    [ -S "${PILOT_SOCKET:-/tmp/pilot.sock}" ] && break
    sleep 0.5
  done
fi
```
