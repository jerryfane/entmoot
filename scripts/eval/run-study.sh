#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/eval/lib.sh
source "$script_dir/lib.sh"

script_name="$(basename "$0")"
mode="run"
group_id=""
node_label="local-vps"
run_id=""
anchor=""
invite_path=""
topic="eval/smoke"
transcript_topics="eval/smoke,eval/pilot,eval/esp-boundary,eval/code-of-conduct,eval/benchmark-plan,eval/discovery-moderation,eval/residential-edge"
transcript_topics_overridden=0
transcript_limit="500"
probe=0
probe_timeout="3s"
with_join=0
with_search=0
with_package=1
include_screenshots=0
esp_url="${ENTMOOT_ESP_URL:-}"
client_id="evaluation-operator"
hermes_container=""
deimos_container=""

usage() {
  cat <<'USAGE'
Usage: run-study.sh [run|plan] --group GROUP_ID [options]

Coordinate the Entmoot live-evaluation helper scripts from one operator command.
The runner is intentionally conservative: it can execute local runtime phases,
and it prints remote/container commands for manual execution inside those
runtimes.

Modes:
  run                         run local phases and package artifacts (default)
  plan                        print local and container command sequence only

Options:
  --group GROUP_ID            controlled evaluation moot group id
  --node-label LABEL          local runtime label (default: local-vps)
  --run-id ID                 evaluation run id; defaults to EVAL_RUN_ID or timestamp
  --anchor TEXT               deterministic smoke/search anchor
  --invite PATH               invite path/link for local join step
  --join                      run local join step; requires --invite
  --probe                     use active doctor/peer probes during baseline
  --timeout DURATION          probe timeout passed to baseline (default: 3s)
  --topic TOPIC               smoke/search topic (default: eval/smoke)
  --transcript-topics LIST    comma-separated transcript topics
  --transcript-limit N        transcript query limit (default: 500)
  --with-search               run ESP search/context check; requires ESP URL/token
  --esp-url URL               ESP base URL; defaults to ENTMOOT_ESP_URL
  --client-id ID              ESP client id (default: evaluation-operator)
  --no-package                skip package-artifacts.sh
  --include-screenshots       include screenshots in the package
  --hermes-container NAME     print Hermes docker exec plan line
  --deimos-container NAME     print Deimos/OpenClaw docker exec plan line
  -h, --help                  show this help

Environment:
  EVAL_RUN_ID                 optional run id
  ENTMOOT_ESP_URL             optional ESP base URL
  ENTMOOT_ESP_TOKEN           required by search-context-check.sh when --with-search
  EVAL_ESP_TOKEN              alternate ESP token env var

Examples:
  scripts/eval/run-study.sh run --group "$GROUP_ID" --probe
  scripts/eval/run-study.sh run --group "$GROUP_ID" --with-search --esp-url "$ENTMOOT_ESP_URL"
  scripts/eval/run-study.sh plan --group "$GROUP_ID" --hermes-container hermes --deimos-container openclaw
USAGE
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    run|plan)
      mode="$1"
      shift
      ;;
  esac
fi

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
    --run-id)
      eval_require_value "$script_name" "$1" "${2:-}"
      run_id="$2"
      shift 2
      ;;
    --anchor)
      eval_require_value "$script_name" "$1" "${2:-}"
      anchor="$2"
      shift 2
      ;;
    --invite)
      eval_require_value "$script_name" "$1" "${2:-}"
      invite_path="$2"
      shift 2
      ;;
    --join)
      with_join=1
      shift
      ;;
    --probe)
      probe=1
      shift
      ;;
    --timeout)
      eval_require_value "$script_name" "$1" "${2:-}"
      probe_timeout="$2"
      shift 2
      ;;
    --topic)
      eval_require_value "$script_name" "$1" "${2:-}"
      topic="$2"
      shift 2
      ;;
    --transcript-topics)
      eval_require_value "$script_name" "$1" "${2:-}"
      transcript_topics="$2"
      transcript_topics_overridden=1
      shift 2
      ;;
    --transcript-limit)
      eval_require_value "$script_name" "$1" "${2:-}"
      transcript_limit="$2"
      shift 2
      ;;
    --with-search)
      with_search=1
      shift
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
    --no-package)
      with_package=0
      shift
      ;;
    --include-screenshots)
      include_screenshots=1
      shift
      ;;
    --hermes-container)
      eval_require_value "$script_name" "$1" "${2:-}"
      hermes_container="$2"
      shift 2
      ;;
    --deimos-container)
      eval_require_value "$script_name" "$1" "${2:-}"
      deimos_container="$2"
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
case "$transcript_limit" in
  ''|*[!0-9]*) eval_usage_error "$script_name" "--transcript-limit must be a positive integer" ;;
esac
transcript_limit=$((10#$transcript_limit))
(( transcript_limit > 0 )) || eval_usage_error "$script_name" "--transcript-limit must be a positive integer"
if (( with_join )) && [[ -z "$invite_path" ]]; then
  eval_usage_error "$script_name" "--join requires --invite"
fi
if (( with_search )) && [[ -z "$esp_url" ]]; then
  eval_usage_error "$script_name" "--with-search requires --esp-url or ENTMOOT_ESP_URL"
fi
if [[ "$mode" == "run" ]] && (( with_search )) && [[ -z "${ENTMOOT_ESP_TOKEN:-${EVAL_ESP_TOKEN:-}}" ]]; then
  eval_usage_error "$script_name" "--with-search requires ENTMOOT_ESP_TOKEN or EVAL_ESP_TOKEN"
fi

repo_root="$(eval_repo_root)"
if [[ -z "$run_id" ]]; then
  run_id="$(eval_run_id)"
else
  run_id="$(eval_safe_component "$run_id")"
fi
if [[ -z "$anchor" ]]; then
  anchor="EVAL-STUDY-ANCHOR-$run_id-$(eval_safe_label "$node_label")"
fi
if (( ! transcript_topics_overridden )); then
  case ",$transcript_topics," in
    *,"$topic",*) ;;
    *) transcript_topics="$topic,$transcript_topics" ;;
  esac
fi

run_dir="$repo_root/artifacts/evaluation/$run_id"
orchestrator_dir="$run_dir/$(eval_safe_label "$node_label")/orchestrator"
summary_path="$orchestrator_dir/summary.txt"

phase_out_dir() {
  printf '%s\n' "$run_dir/$(eval_safe_label "$node_label")/$1"
}

redacted_invite_ref() {
  if [[ "$invite_path" == entmoot://* ]]; then
    printf '%s\n' "<invite:entmoot_link:redacted>"
  elif [[ "$invite_path" == http://* || "$invite_path" == https://* ]]; then
    printf '%s\n' "<invite:url:redacted>"
  else
    printf '%s\n' "<invite:file:$(basename "$invite_path")>"
  fi
}

quote_cmd() {
  printf '%q ' "$@"
  printf '\n'
}

print_header() {
  printf '\n== %s ==\n' "$1"
}

print_plan_command() {
  local label="$1"
  shift
  printf '%s\n' "$label"
  printf '  '
  quote_cmd "$@"
}

print_plan() {
  local -a baseline_plan_args package_plan_args
  baseline_plan_args=(--group "$group_id" --node-label "$node_label")
  if (( probe )); then
    baseline_plan_args+=(--probe --timeout "$probe_timeout")
  fi
  package_plan_args=(--run-dir "$run_dir")
  if (( include_screenshots )); then
    package_plan_args+=(--include-screenshots)
  fi

  cat <<PLAN
Evaluation plan
  run_id: $run_id
  group: $group_id
  local_label: $node_label
  anchor: $anchor
  topic: $topic
PLAN

  print_header "local runtime"
  print_plan_command "baseline before joins:" \
    env EVAL_RUN_ID="$run_id" "$script_dir/baseline.sh" "${baseline_plan_args[@]}" --out-dir "$(phase_out_dir baseline-before)"
  if (( with_join )); then
    print_plan_command "local join:" \
      env EVAL_RUN_ID="$run_id" "$script_dir/join-controlled-moot.sh" --invite "$(redacted_invite_ref)" --group "$group_id" --node-label "$node_label" --out-dir "$(phase_out_dir join)"
    print_plan_command "baseline after join:" \
      env EVAL_RUN_ID="$run_id" "$script_dir/baseline.sh" "${baseline_plan_args[@]}" --out-dir "$(phase_out_dir baseline-after-join)"
  fi
  print_plan_command "smoke publish:" \
    env EVAL_RUN_ID="$run_id" "$script_dir/publish-smoke.sh" --group "$group_id" --node-label "$node_label" --topic "$topic" --anchor "$anchor" --out-dir "$(phase_out_dir smoke)"
  if (( with_search )); then
    print_plan_command "ESP search/context:" \
      env EVAL_RUN_ID="$run_id" "$script_dir/search-context-check.sh" --group "$group_id" --node-label "$node_label" --topic "$topic" --anchor "$anchor" --esp-url "$esp_url" --client-id "$client_id" --out-dir "$(phase_out_dir search-context)"
  fi
  print_plan_command "transcript export:" \
    env EVAL_RUN_ID="$run_id" "$script_dir/export-transcript.sh" --group "$group_id" --node-label "$node_label" --topic "$transcript_topics" --limit "$transcript_limit" --out-dir "$(phase_out_dir transcript)"
  if (( with_package )); then
    print_plan_command "artifact package:" \
      "$script_dir/package-artifacts.sh" "${package_plan_args[@]}"
  fi

  if [[ -n "$hermes_container" || -n "$deimos_container" ]]; then
    print_header "container runtime examples"
  fi
  if [[ -n "$hermes_container" ]]; then
    print_plan_command "Hermes baseline:" \
      docker exec -e EVAL_RUN_ID="$run_id" "$hermes_container" /path/to/entmoot/scripts/eval/baseline.sh --group "$group_id" --node-label hermes-container --probe
  fi
  if [[ -n "$deimos_container" ]]; then
    print_plan_command "Deimos/OpenClaw baseline:" \
      docker exec -e EVAL_RUN_ID="$run_id" "$deimos_container" /path/to/entmoot/scripts/eval/baseline.sh --group "$group_id" --node-label deimos-openclaw-container --probe
  fi
}

record_summary_start() {
  mkdir -p "$orchestrator_dir"
  {
    printf 'run_id=%s\n' "$run_id"
    printf 'group_id=%s\n' "$group_id"
    printf 'node_label=%s\n' "$node_label"
    printf 'anchor=%s\n' "$anchor"
    printf 'topic=%s\n' "$topic"
    printf 'transcript_topics=%s\n' "$transcript_topics"
    printf 'with_join=%s\n' "$with_join"
    printf 'with_search=%s\n' "$with_search"
    printf 'with_package=%s\n' "$with_package"
    printf 'created_utc=%s\n' "$(eval_timestamp_utc)"
    printf '\n'
  } >"$summary_path"
}

run_step() {
  local label="$1"
  shift
  print_header "$label"
  {
    printf 'step_start=%s %s\n' "$label" "$(eval_timestamp_utc)"
    printf 'command='
    printf '%q ' "$@"
    printf '\n'
  } >>"$summary_path"

  if "$@"; then
    printf 'step_status=%s ok\n\n' "$label" >>"$summary_path"
  else
    local status=$?
    printf 'step_status=%s failed exit=%s\n\n' "$label" "$status" >>"$summary_path"
    return "$status"
  fi
}

run_join_step() {
  local label="join"
  print_header "$label"
  {
    printf 'step_start=%s %s\n' "$label" "$(eval_timestamp_utc)"
    printf 'command='
    printf '%q ' "$script_dir/join-controlled-moot.sh" --invite "$(redacted_invite_ref)" --group "$group_id" --node-label "$node_label" --out-dir "$(phase_out_dir join)"
    printf '\n'
  } >>"$summary_path"

  if "$script_dir/join-controlled-moot.sh" --invite "$invite_path" --group "$group_id" --node-label "$node_label" --out-dir "$(phase_out_dir join)"; then
    printf 'step_status=%s ok\n\n' "$label" >>"$summary_path"
  else
    local status=$?
    printf 'step_status=%s failed exit=%s\n\n' "$label" "$status" >>"$summary_path"
    return "$status"
  fi
}

if [[ "$mode" == "plan" ]]; then
  print_plan
  exit 0
fi

export EVAL_RUN_ID="$run_id"
record_summary_start
print_plan | tee -a "$summary_path"

baseline_args=(--group "$group_id" --node-label "$node_label")
if (( probe )); then
  baseline_args+=(--probe --timeout "$probe_timeout")
fi

run_step "baseline-before" "$script_dir/baseline.sh" "${baseline_args[@]}" --out-dir "$(phase_out_dir baseline-before)"
if (( with_join )); then
  run_join_step
  run_step "baseline-after-join" "$script_dir/baseline.sh" "${baseline_args[@]}" --out-dir "$(phase_out_dir baseline-after-join)"
fi

run_step "smoke" "$script_dir/publish-smoke.sh" \
  --group "$group_id" \
  --node-label "$node_label" \
  --topic "$topic" \
  --anchor "$anchor" \
  --out-dir "$(phase_out_dir smoke)"

if (( with_search )); then
  run_step "search-context" "$script_dir/search-context-check.sh" \
    --group "$group_id" \
    --node-label "$node_label" \
    --topic "$topic" \
    --anchor "$anchor" \
    --esp-url "$esp_url" \
    --client-id "$client_id" \
    --out-dir "$(phase_out_dir search-context)"
fi

run_step "transcript" "$script_dir/export-transcript.sh" \
  --group "$group_id" \
  --node-label "$node_label" \
  --topic "$transcript_topics" \
  --limit "$transcript_limit" \
  --out-dir "$(phase_out_dir transcript)"

if (( with_package )); then
  package_args=(--run-dir "$run_dir")
  if (( include_screenshots )); then
    package_args+=(--include-screenshots)
  fi
  run_step "package" "$script_dir/package-artifacts.sh" "${package_args[@]}"
fi

{
  printf 'completed_utc=%s\n' "$(eval_timestamp_utc)"
  printf 'run_dir=%s\n' "$run_dir"
} >>"$summary_path"

print_header "complete"
echo "run id: $run_id"
echo "anchor: $anchor"
echo "run dir: $run_dir"
echo "summary: $summary_path"
