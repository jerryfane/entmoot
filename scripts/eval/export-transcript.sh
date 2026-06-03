#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/eval/lib.sh
source "$script_dir/lib.sh"

script_name="$(basename "$0")"
group_id=""
node_label=""
out_dir=""
limit="200"
topics=()

usage() {
  cat <<'USAGE'
Usage: export-transcript.sh --group GROUP_ID --node-label LABEL [--topic TOPIC] [--limit N] [--out-dir DIR]

Export Entmoot messages for evaluation evidence. The script writes raw JSONL
exactly as returned by entmootd query and a readable Markdown transcript under
an ignored artifacts/evaluation run directory unless --out-dir is provided.

Options:
  --group GROUP_ID       Entmoot group id to query
  --node-label LABEL     stable operator label for this runtime
  --topic TOPIC          topic filter; repeatable and comma-separated values are accepted
  --limit N              maximum messages per query (default: 200)
  --out-dir DIR          output directory override
  -h, --help             show this help

Environment:
  EVAL_RUN_ID            optional run id for default artifact paths
  ENTMOOTD               entmootd or wrapper path
  ENTMOOT_AGENT_WRAPPER  optional wrapper path (default: /data/.entmoot/entmoot)
  ENTMOOT_DATA           optional Entmoot data root passed as -data
  ENTMOOT_IDENTITY       optional Entmoot identity path passed as -identity
  PILOT_SOCKET           optional Pilot IPC socket path passed as -socket
USAGE
}

add_topics() {
  local raw="$1"
  local part
  local -a split_topics
  IFS=',' read -r -a split_topics <<<"$raw"
  for part in "${split_topics[@]}"; do
    part="${part#"${part%%[![:space:]]*}"}"
    part="${part%"${part##*[![:space:]]}"}"
    if [[ -n "$part" ]]; then
      topics+=("$part")
    fi
  done
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
      add_topics "$2"
      shift 2
      ;;
    --limit)
      eval_require_value "$script_name" "$1" "${2:-}"
      limit="$2"
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
case "$limit" in
  ''|*[!0-9]*) eval_usage_error "$script_name" "--limit must be a positive integer" ;;
esac
(( limit > 0 )) || eval_usage_error "$script_name" "--limit must be a positive integer"

if (( ${#topics[@]} == 0 )); then
  topics=("")
fi

eval_require_command jq
eval_init_entmoot
run_id="$(eval_run_id)"
out_dir="$(eval_prepare_out_dir transcript "$node_label" "$out_dir" "$run_id")"
markdown_path="$out_dir/transcript.md"
failures=0

{
  printf 'run_id=%s\n' "$run_id"
  printf 'phase=transcript\n'
  printf 'node_label=%s\n' "$node_label"
  printf 'group_id=%s\n' "$group_id"
  printf 'limit=%s\n' "$limit"
  printf 'created_utc=%s\n' "$(eval_timestamp_utc)"
  printf 'entmoot_base='
  printf '%q ' "${eval_entmoot_base[@]}"
  printf '\n'
  printf 'topics='
  printf '%q ' "${topics[@]}"
  printf '\n'
} >"$out_dir/run.meta"

{
  printf '# Entmoot Evaluation Transcript\n\n'
  printf '%s\n' "- Group: \`$group_id\`"
  printf '%s\n' "- Node label: \`$node_label\`"
  printf '%s\n' "- Run ID: \`$run_id\`"
  printf '%s\n\n' "- Exported UTC: \`$(eval_timestamp_utc)\`"
} >"$markdown_path"

render_markdown() {
  local raw_path="$1"
  local topic_label="$2"

  jq -r --arg topic "$topic_label" '
    def ts:
      if (.timestamp_ms // 0) > 0 then
        ((.timestamp_ms / 1000) | floor | strftime("%Y-%m-%dT%H:%M:%SZ"))
      else
        "unknown-time"
      end;
    def topics:
      if (.topic // []) | type == "array" then
        (.topic | join(", "))
      else
        (.topic // "")
      end;
    "## Topic: " + (if $topic == "" then "all" else $topic end),
    "",
    (
      if length == 0 then
        "_No messages returned._"
      else
        .[] |
        "### " + ts + " - author " + ((.author // "unknown") | tostring),
        "",
        "- Message ID: `" + (.message_id // "") + "`",
        "- Topics: `" + topics + "`",
        "",
        (.content // ""),
        ""
      end
    )
  ' "$raw_path" >>"$markdown_path"
}

for topic in "${topics[@]}"; do
  safe_topic="$(eval_safe_component "${topic:-all}")"
  raw_path="$out_dir/messages-$safe_topic.jsonl"
  json_path="$out_dir/messages-$safe_topic.json"
  label="query-$safe_topic"
  query_cmd=("${eval_entmoot_base[@]}" query -group "$group_id" -limit "$limit" -order asc)
  if [[ -n "$topic" ]]; then
    query_cmd+=(-topic "$topic")
  fi

  echo "== query ${topic:-all} =="
  if eval_run_capture "$label" "$out_dir" "${query_cmd[@]}"; then
    mv "$out_dir/$label.stdout" "$raw_path"
    echo "ok: query ${topic:-all}"
  else
    status=$?
    mv "$out_dir/$label.stdout" "$raw_path"
    echo "failed: query ${topic:-all} (exit $status)" >&2
    failures=$((failures + 1))
  fi

  if [[ -s "$raw_path" ]]; then
    if jq -s . "$raw_path" >"$json_path"; then
      render_markdown "$json_path" "$topic"
    else
      echo "failed: convert JSONL for ${topic:-all}" >&2
      failures=$((failures + 1))
    fi
  else
    printf '[]\n' >"$json_path"
    render_markdown "$json_path" "$topic"
  fi
done

echo
echo "transcript output: $out_dir"
echo "markdown: $markdown_path"

if (( failures > 0 )); then
  echo "export-transcript.sh: $failures command(s) failed; captured output remains in $out_dir" >&2
  exit 1
fi
