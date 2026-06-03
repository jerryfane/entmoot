#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/eval/lib.sh
source "$script_dir/lib.sh"

script_name="$(basename "$0")"
group_id=""
node_label=""
topic="eval/smoke"
anchor=""
out_dir=""

usage() {
  cat <<'USAGE'
Usage: publish-smoke.sh --group GROUP_ID --node-label LABEL [--topic TOPIC] [--anchor ANCHOR] [--out-dir DIR]

Publish deterministic smoke messages for the live evaluation. The script sends
three messages on the selected topic: a pre-anchor neighbor, the anchor message,
and a post-anchor neighbor. Raw command output is written under the ignored
evaluation artifact directory unless --out-dir is provided.

Options:
  --group GROUP_ID    Entmoot group id to publish into
  --node-label LABEL  stable operator label for this runtime
  --topic TOPIC       publish topic (default: eval/smoke)
  --anchor ANCHOR     anchor text for later search/context checks
  --out-dir DIR       output directory override
  -h, --help          show this help

Environment:
  EVAL_RUN_ID            optional run id for default artifact paths
  ENTMOOTD               entmootd or wrapper path
  ENTMOOT_AGENT_WRAPPER  optional wrapper path (default: /data/.entmoot/entmoot)
  ENTMOOT_DATA           optional Entmoot data root passed as -data
  ENTMOOT_IDENTITY       optional Entmoot identity path passed as -identity
  PILOT_SOCKET           optional Pilot IPC socket path passed as -socket
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --group)
      eval_require_value "$script_name" "$1" "${2:-}"
      group_id="$2"
      shift 2
      ;;
    --node-label)
      eval_require_value "$script_name" "$1" "${2:-}"
      node_label="$2"
      shift 2
      ;;
    --topic)
      eval_require_value "$script_name" "$1" "${2:-}"
      topic="$2"
      shift 2
      ;;
    --anchor)
      eval_require_value "$script_name" "$1" "${2:-}"
      anchor="$2"
      shift 2
      ;;
    --out-dir)
      eval_require_value "$script_name" "$1" "${2:-}"
      out_dir="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      eval_usage_error "$script_name" "unknown argument: $1"
      ;;
  esac
done

[[ -n "$group_id" ]] || eval_usage_error "$script_name" "--group is required"
[[ -n "$node_label" ]] || eval_usage_error "$script_name" "--node-label is required"

eval_init_entmoot
run_id="$(eval_run_id)"
if [[ -z "$anchor" ]]; then
  anchor="EVAL-CONTEXT-ANCHOR-$(eval_timestamp_utc)-$(eval_safe_label "$node_label")"
fi
out_dir="$(eval_prepare_out_dir smoke "$node_label" "$out_dir" "$run_id")"

{
  printf 'run_id=%s\n' "$run_id"
  printf 'phase=smoke\n'
  printf 'node_label=%s\n' "$node_label"
  printf 'group_id=%s\n' "$group_id"
  printf 'topic=%s\n' "$topic"
  printf 'anchor=%s\n' "$anchor"
  printf 'created_utc=%s\n' "$(eval_timestamp_utc)"
  printf 'entmoot_base='
  printf '%q ' "${eval_entmoot_base[@]}"
  printf '\n'
} >"$out_dir/run.meta"

messages_meta="$out_dir/messages.meta"
: >"$messages_meta"

publish_message() {
  local label="$1"
  local content="$2"
  local sent_utc

  sent_utc="$(eval_timestamp_utc)"
  {
    printf '%s.sent_utc=%s\n' "$label" "$sent_utc"
    printf '%s.topic=%s\n' "$label" "$topic"
    printf '%s.anchor=%s\n' "$label" "$anchor"
    printf '%s.sha256=%s\n' "$label" "$(eval_sha256_text "$content")"
  } >>"$messages_meta"

  echo "== publish $label =="
  if eval_run_capture_stdin "$label" "$out_dir" "$content" \
    "${eval_entmoot_base[@]}" publish -group "$group_id" -topic "$topic" -file -; then
    echo "ok: publish $label"
  else
    local status=$?
    echo "failed: publish $label (exit $status)" >&2
    return "$status"
  fi
}

failures=0
pre_content="Entmoot evaluation smoke pre-anchor for $anchor"
anchor_content="Entmoot evaluation smoke anchor: $anchor"
post_content="Entmoot evaluation smoke post-anchor for $anchor"

publish_message pre_anchor "$pre_content" || failures=$((failures + 1))
publish_message anchor "$anchor_content" || failures=$((failures + 1))
publish_message post_anchor "$post_content" || failures=$((failures + 1))

echo
echo "anchor: $anchor"
echo "smoke output: $out_dir"

if (( failures > 0 )); then
  echo "publish-smoke.sh: $failures publish command(s) failed; captured output remains in $out_dir" >&2
  exit 1
fi
