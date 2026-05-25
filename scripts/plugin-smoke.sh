#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO_BIN:-go}"
CREATED_WORK_DIR=""
if [[ -n "${ENTMOOT_PLUGIN_SMOKE_DIR:-}" ]]; then
  WORK_DIR="$ENTMOOT_PLUGIN_SMOKE_DIR"
else
  WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/entmoot-plugin-smoke.XXXXXX")"
  CREATED_WORK_DIR=1
fi
KEEP_WORK_DIR="${ENTMOOT_PLUGIN_SMOKE_KEEP:-}"

cleanup() {
  if [[ -n "$CREATED_WORK_DIR" && -z "$KEEP_WORK_DIR" && -d "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

mkdir -p "$WORK_DIR"

BIN="${ENTMOOTD_BIN:-$WORK_DIR/entmootd}"
ENTMOOT_HOME="$WORK_DIR/entmoot-home"
RUNTIME_HOME="$WORK_DIR/runtime-home"
CODEX_OUT="$WORK_DIR/codex-plugin"
CLAUDE_OUT="$WORK_DIR/claude-plugin"

rm -rf "$ENTMOOT_HOME" "$RUNTIME_HOME" "$CODEX_OUT" "$CLAUDE_OUT"
mkdir -p "$ENTMOOT_HOME" "$RUNTIME_HOME" "$RUNTIME_HOME/.codex"

if [[ -z "${ENTMOOTD_BIN:-}" ]]; then
  (cd "$ROOT_DIR/src" && "$GO_BIN" build -o "$BIN" ./cmd/entmootd)
fi

"$BIN" plugin build codex --out "$CODEX_OUT"
"$BIN" plugin build claude --out "$CLAUDE_OUT"

assert_skill_tree() {
  local package_dir="$1"
  test -f "$package_dir/skills/entmoot/SKILL.md"
  while IFS= read -r ref; do
    local rel="${ref#"$ROOT_DIR/src/skills/entmoot/"}"
    test -f "$package_dir/skills/entmoot/$rel"
  done < <(find "$ROOT_DIR/src/skills/entmoot/references" -type f | sort)
}

assert_skill_tree "$CODEX_OUT"
assert_skill_tree "$CLAUDE_OUT"

HAS_CLAUDE=""
HAS_CODEX=""
if command -v claude >/dev/null 2>&1; then
  HAS_CLAUDE=1
  HOME="$RUNTIME_HOME" claude plugin validate "$CLAUDE_OUT"
else
  echo "skip: claude CLI not found; package validation skipped"
fi

if command -v codex >/dev/null 2>&1; then
  HAS_CODEX=1
  HOME="$RUNTIME_HOME" CODEX_HOME="$RUNTIME_HOME/.codex" codex plugin marketplace list >/dev/null
  HOME="$RUNTIME_HOME" CODEX_HOME="$RUNTIME_HOME/.codex" codex plugin list >/dev/null
else
  echo "skip: codex CLI not found; non-mutating codex plugin checks skipped"
fi

"$BIN" plugin build codex --home "$ENTMOOT_HOME"
"$BIN" plugin build claude --home "$ENTMOOT_HOME"

if [[ -n "$HAS_CODEX" ]]; then
  "$BIN" plugin doctor codex --home "$ENTMOOT_HOME"
else
  "$BIN" plugin doctor codex --home "$ENTMOOT_HOME" || true
fi

if [[ -n "$HAS_CLAUDE" ]]; then
  HOME="$RUNTIME_HOME" "$BIN" plugin doctor claude --home "$ENTMOOT_HOME"
else
  "$BIN" plugin doctor claude --home "$ENTMOOT_HOME" || true
fi

HOME="$RUNTIME_HOME" CODEX_HOME="$RUNTIME_HOME/.codex" "$BIN" plugin install codex --home "$ENTMOOT_HOME" --force
HOME="$RUNTIME_HOME" "$BIN" plugin install claude --home "$ENTMOOT_HOME" --scope user --force

if [[ -n "$HAS_CODEX$HAS_CLAUDE" ]]; then
  HOME="$RUNTIME_HOME" CODEX_HOME="$RUNTIME_HOME/.codex" "$BIN" plugin doctor --home "$ENTMOOT_HOME"
else
  "$BIN" plugin doctor --home "$ENTMOOT_HOME" || true
fi

assert_skill_tree "$ENTMOOT_HOME/plugins/build/codex/entmoot"
assert_skill_tree "$ENTMOOT_HOME/plugins/build/claude/entmoot"

if [[ -n "$HAS_CODEX" ]]; then
  test -f "$RUNTIME_HOME/.codex/config.toml"
fi
if [[ -n "$HAS_CLAUDE" ]]; then
  test -f "$RUNTIME_HOME/.claude/plugins/installed_plugins.json"
fi

echo "plugin smoke passed"
echo "work dir: $WORK_DIR"
