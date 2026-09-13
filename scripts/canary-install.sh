#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
bin="${ENTMOOTD:-$work/entmootd}"
if [[ -z "${ENTMOOTD:-}" ]]; then
  (cd "$repo_root/src" && go build -o "$bin" ./cmd/entmootd)
fi
# Never read or write the operator's installation, keys, or shell profiles.
export HOME="$work/home"
export ENTMOOT_HOME="$work/custom home's installation"
mkdir -p "$HOME"
unset ENTMOOT_RUNTIME_ENV ENTMOOT_BIN ENTMOOT_DATA ENTMOOT_IDENTITY
ENTMOOTD_LOCAL_BIN="$bin" sh "$repo_root/install.sh"
"$ENTMOOT_HOME/entmoot" -allow-new-identity info > "$work/direct.json"
"$ENTMOOT_HOME/bin/entmoot" info > "$work/symlink.json"
"${PYTHON:-python3}" - "$work/direct.json" "$work/symlink.json" "$ENTMOOT_HOME" "$HOME" <<'PY'
import json
from pathlib import Path
import sys
first, second = (json.loads(Path(p).read_text()) for p in sys.argv[1:3])
assert first['member_id'] == second['member_id'], (first, second)
assert Path(sys.argv[3], 'identity.json').is_file()
assert not Path(sys.argv[4], '.entmoot').exists()
PY
# The explicit runtime-file override remains supported independently of HOME.
cp "$ENTMOOT_HOME/runtime.env" "$work/runtime.env"
ENTMOOT_RUNTIME_ENV="$work/runtime.env" "$ENTMOOT_HOME/bin/entmoot" info > "$work/override.json"
cmp "$work/direct.json" "$work/override.json"
printf 'installer canary passed: isolated HOME, custom installation, quoted paths, wrapper/symlink, explicit runtime override\n'
