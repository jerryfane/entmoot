#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/eval/lib.sh
source "$script_dir/lib.sh"

script_name="$(basename "$0")"
group_id=""
anchor=""
node_label=""
topic=""
out_dir=""
esp_url=""
client_id=""
limit="10"
before="5"
after="5"
bearer_token="${ENTMOOT_ESP_TOKEN:-${EVAL_ESP_TOKEN:-}}"

usage() {
  cat <<'USAGE'
Usage: search-context-check.sh --group GROUP_ID --anchor TEXT --node-label LABEL --esp-url URL --client-id ID [options]

Verify ESP lexical search and message-context retrieval for a known evaluation
anchor. The script saves raw HTTP responses, timing metadata, and a pass/fail
summary under an ignored artifacts/evaluation run directory unless --out-dir is
provided.

Options:
  --group GROUP_ID       Entmoot group id to query
  --anchor TEXT          unique smoke-message anchor to search for
  --node-label LABEL     stable operator label for this runtime
  --esp-url URL          ESP base URL, for example https://esp.example
  --client-id ID         ESP client id used for read-only search/context calls
  --topic TOPIC          optional topic filter
  --out-dir DIR          output directory override
  --limit N              search result limit (default: 10)
  --before N             context messages before target; must be positive (default: 5)
  --after N              context messages after target; must be positive (default: 5)
  --bearer-token TOKEN   ESP bearer token; defaults to ENTMOOT_ESP_TOKEN or EVAL_ESP_TOKEN
  -h, --help             show this help

Environment:
  EVAL_RUN_ID            optional run id for default artifact paths
  ENTMOOT_ESP_TOKEN      optional bearer token for ESP HTTP auth
  EVAL_ESP_TOKEN         optional bearer token for ESP HTTP auth

Notes:
  This build does not expose local entmootd search/context subcommands, so this
  script uses ESP HTTP endpoints. It requires bearer auth because all /v1 ESP
  routes are authenticated in the normal deployment.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --group)
      eval_require_value "$script_name" "$1" "${2:-}"
      group_id="$2"
      shift 2
      ;;
    --anchor)
      eval_require_value "$script_name" "$1" "${2:-}"
      anchor="$2"
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
    --out-dir)
      eval_require_value "$script_name" "$1" "${2:-}"
      out_dir="$2"
      shift 2
      ;;
    --esp-url)
      eval_require_value "$script_name" "$1" "${2:-}"
      esp_url="$2"
      shift 2
      ;;
    --client-id)
      eval_require_value "$script_name" "$1" "${2:-}"
      client_id="$2"
      shift 2
      ;;
    --limit)
      eval_require_value "$script_name" "$1" "${2:-}"
      limit="$2"
      shift 2
      ;;
    --before)
      eval_require_value "$script_name" "$1" "${2:-}"
      before="$2"
      shift 2
      ;;
    --after)
      eval_require_value "$script_name" "$1" "${2:-}"
      after="$2"
      shift 2
      ;;
    --bearer-token)
      eval_require_value "$script_name" "$1" "${2:-}"
      bearer_token="$2"
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
[[ -n "$anchor" ]] || eval_usage_error "$script_name" "--anchor is required"
[[ -n "$node_label" ]] || eval_usage_error "$script_name" "--node-label is required"
[[ -n "$esp_url" ]] || eval_usage_error "$script_name" "--esp-url is required"
[[ -n "$client_id" ]] || eval_usage_error "$script_name" "--client-id is required"
[[ -n "$bearer_token" ]] || eval_usage_error "$script_name" "ENTMOOT_ESP_TOKEN, EVAL_ESP_TOKEN, or --bearer-token is required"

eval_require_command curl
eval_require_command jq

case "$limit" in
  ''|*[!0-9]*) eval_usage_error "$script_name" "--limit must be a positive integer" ;;
esac
case "$before" in
  ''|*[!0-9]*) eval_usage_error "$script_name" "--before must be a positive integer" ;;
esac
case "$after" in
  ''|*[!0-9]*) eval_usage_error "$script_name" "--after must be a positive integer" ;;
esac
(( limit > 0 )) || eval_usage_error "$script_name" "--limit must be a positive integer"
(( before > 0 )) || eval_usage_error "$script_name" "--before must be a positive integer"
(( after > 0 )) || eval_usage_error "$script_name" "--after must be a positive integer"

url_encode() {
  jq -rn --arg value "$1" '$value|@uri'
}

http_get_capture() {
  local label="$1"
  local path_query="$2"
  local stdout_path="$out_dir/$label.json"
  local stderr_path="$out_dir/$label.stderr"
  local status_path="$out_dir/$label.status"
  local meta_path="$out_dir/$label.meta"
  local start_ts end_ts status
  local url="${esp_url%/}$path_query"

  start_ts="$(eval_timestamp_utc)"
  if curl -fsS --config "$curl_auth_config" "$url" >"$stdout_path" 2>"$stderr_path"; then
    status=0
  else
    status=$?
  fi
  end_ts="$(eval_timestamp_utc)"

  printf '%s\n' "$status" >"$status_path"
  eval_write_command_meta "$meta_path" "$label" "$start_ts" "$end_ts" "$status" \
    curl -fsS -H "Authorization: Bearer <redacted>" "$url"
  return "$status"
}

run_id="$(eval_run_id)"
out_dir="$(eval_prepare_out_dir search-context "$node_label" "$out_dir" "$run_id")"
summary_path="$out_dir/summary.txt"
search_path="$out_dir/search.json"
context_path="$out_dir/context.json"
cursor_before_path="$out_dir/cursor-before.json"
cursor_after_path="$out_dir/cursor-after.json"
failures=0
curl_auth_config="$(mktemp "${TMPDIR:-/tmp}/entmoot-eval-curl.XXXXXX")"
chmod 600 "$curl_auth_config"
escaped_bearer_token="${bearer_token//\\/\\\\}"
escaped_bearer_token="${escaped_bearer_token//\"/\\\"}"
printf 'header = "Authorization: Bearer %s"\n' "$escaped_bearer_token" >"$curl_auth_config"
unset escaped_bearer_token
cleanup_auth_config() {
  rm -f "$curl_auth_config"
}
trap cleanup_auth_config EXIT

group_enc="$(url_encode "$group_id")"
client_enc="$(url_encode "$client_id")"
anchor_enc="$(url_encode "$anchor")"
topic_query=""
if [[ -n "$topic" ]]; then
  topic_query="&topic=$(url_encode "$topic")"
fi

{
  printf 'run_id=%s\n' "$run_id"
  printf 'phase=search-context\n'
  printf 'node_label=%s\n' "$node_label"
  printf 'group_id=%s\n' "$group_id"
  printf 'anchor=%s\n' "$anchor"
  printf 'topic=%s\n' "$topic"
  printf 'esp_url=%s\n' "${esp_url%/}"
  printf 'client_id=%s\n' "$client_id"
  printf 'limit=%s\n' "$limit"
  printf 'before=%s\n' "$before"
  printf 'after=%s\n' "$after"
  printf 'created_utc=%s\n' "$(eval_timestamp_utc)"
} >"$out_dir/run.meta"

echo "== cursor before =="
if http_get_capture cursor-before "/v1/mailbox/cursor?client_id=$client_enc&group_id=$group_enc"; then
  echo "ok: cursor before"
else
  status=$?
  echo "failed: cursor before (exit $status)" >&2
  failures=$((failures + 1))
fi

echo "== search =="
if http_get_capture search "/v1/groups/$group_enc/search?client_id=$client_enc&q=$anchor_enc&limit=$limit$topic_query"; then
  echo "ok: search"
else
  status=$?
  echo "failed: search (exit $status)" >&2
  failures=$((failures + 1))
fi

target_message_id=""
if [[ -s "$search_path" ]] && jq -e . "$search_path" >/dev/null 2>&1; then
  target_message_id="$(
    jq -r --arg anchor "$anchor" '
      first(.results[]? | select(.message.content == ("Entmoot evaluation smoke anchor: " + $anchor)) | .message.message_id)
      // first(.results[]? | select((.message.content // "") | contains($anchor)) | .message.message_id)
      // ""
    ' "$search_path"
  )"
fi

if [[ -z "$target_message_id" || "$target_message_id" == "null" ]]; then
  echo "failed: search did not return an anchor message" >&2
  failures=$((failures + 1))
else
  message_enc="$(url_encode "$target_message_id")"
  echo "== message context =="
  if http_get_capture context "/v1/groups/$group_enc/message-context?client_id=$client_enc&message_id=$message_enc&before=$before&after=$after$topic_query"; then
    echo "ok: message context"
  else
    status=$?
    echo "failed: message context (exit $status)" >&2
    failures=$((failures + 1))
  fi
fi

echo "== cursor after =="
if http_get_capture cursor-after "/v1/mailbox/cursor?client_id=$client_enc&group_id=$group_enc"; then
  echo "ok: cursor after"
else
  status=$?
  echo "failed: cursor after (exit $status)" >&2
  failures=$((failures + 1))
fi

search_has_anchor=0
context_has_target=0
context_has_pre=0
context_has_anchor=0
context_has_post=0
cursor_unchanged=0

if [[ -s "$search_path" ]] && jq -e --arg anchor "$anchor" \
  'any(.results[]?; (.message.content // "") | contains($anchor))' "$search_path" >/dev/null; then
  search_has_anchor=1
fi

if [[ -s "$context_path" && -n "$target_message_id" && "$target_message_id" != "null" ]]; then
  if jq -e --arg id "$target_message_id" \
    '(.target_message_id == $id) and any(.messages[]?; .message_id == $id)' "$context_path" >/dev/null; then
    context_has_target=1
  fi
  if jq -e --arg anchor "$anchor" \
    'any(.messages[]?; (.content // "") == ("Entmoot evaluation smoke pre-anchor for " + $anchor))' "$context_path" >/dev/null; then
    context_has_pre=1
  fi
  if jq -e --arg anchor "$anchor" \
    'any(.messages[]?; (.content // "") == ("Entmoot evaluation smoke anchor: " + $anchor))' "$context_path" >/dev/null; then
    context_has_anchor=1
  fi
  if jq -e --arg anchor "$anchor" \
    'any(.messages[]?; (.content // "") == ("Entmoot evaluation smoke post-anchor for " + $anchor))' "$context_path" >/dev/null; then
    context_has_post=1
  fi
fi

if [[ -s "$cursor_before_path" && -s "$cursor_after_path" ]]; then
  before_cursor="$(jq -c '.cursor' "$cursor_before_path" 2>/dev/null || true)"
  after_cursor="$(jq -c '.cursor' "$cursor_after_path" 2>/dev/null || true)"
  if [[ -n "$before_cursor" && "$before_cursor" == "$after_cursor" ]]; then
    cursor_unchanged=1
  fi
fi

{
  printf 'status=%s\n' "fail"
  printf 'target_message_id=%s\n' "$target_message_id"
  printf 'search_has_anchor=%s\n' "$search_has_anchor"
  printf 'context_has_target=%s\n' "$context_has_target"
  printf 'context_has_pre_anchor_neighbor=%s\n' "$context_has_pre"
  printf 'context_has_anchor_message=%s\n' "$context_has_anchor"
  printf 'context_has_post_anchor_neighbor=%s\n' "$context_has_post"
  printf 'cursor_unchanged=%s\n' "$cursor_unchanged"
  printf 'output_dir=%s\n' "$out_dir"
} >"$summary_path"

for check in search_has_anchor context_has_target context_has_pre context_has_anchor context_has_post cursor_unchanged; do
  if [[ "${!check}" != "1" ]]; then
    echo "failed: $check" >&2
    failures=$((failures + 1))
  fi
done

if (( failures == 0 )); then
  sed -i.bak 's/^status=fail$/status=pass/' "$summary_path"
  rm -f "$summary_path.bak"
fi

echo
echo "search/context output: $out_dir"
cat "$summary_path"

if (( failures > 0 )); then
  echo "search-context-check.sh: $failures check(s) failed; captured output remains in $out_dir" >&2
  exit 1
fi
