# Install, Update, And First Checks

Use this reference for first-run environment checks, missing binaries, release
updates, and libp2p connectivity profiles.

## First Checks

```sh
export PATH="$HOME/.entmoot/bin:$PATH"

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

Install a missing binary:

```sh
if [ "$ENTMOOT" != "/data/.entmoot/entmoot" ] && ! command -v entmootd >/dev/null 2>&1; then
  curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh
  export PATH="$HOME/.entmoot/bin:$PATH"
fi

"$ENTMOOT" version
```

Use the release updater when `entmootd version` is older than the required
release, reports `dev`, or when the newest release is needed:

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
"$ENTMOOT" update --restart --tag <release-tag> --install-dir "$ENTMOOT_UPDATE_INSTALL_DIR"
```

## Connectivity Profiles

Direct mode is the default. It exposes a libp2p listener and is appropriate for
publicly reachable servers and peers whose network permits direct connections:

```sh
"$ENTMOOT" -connectivity direct -listen-port 1004 serve
```

Relay-only mode prevents direct application-peer connections and requires at
least one owner-controlled Circuit Relay v2 multiaddr:

```sh
"$ENTMOOT" \
  -connectivity relay-only \
  -controlled-relay '/dns4/relay.example/tcp/4001/p2p/<relay-peer-id>' \
  serve
```

Relay-only mode has no direct or TURN fallback. An unavailable controlled relay
makes the node unavailable by design.
