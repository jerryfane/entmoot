#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
if [[ -n "${ENTMOOTD:-}" ]]; then
  bin="$ENTMOOTD"
else
  bin="$work/entmootd"
  (cd "$repo_root/src" && go build -o "$bin" ./cmd/entmootd)
fi
"${PYTHON:-python3}" "$repo_root/scripts/canary-libp2p.py" "$bin"
