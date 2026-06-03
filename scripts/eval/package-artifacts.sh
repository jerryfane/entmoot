#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/eval/lib.sh
source "$script_dir/lib.sh"

script_name="$(basename "$0")"
run_dir=""
output=""
include_screenshots=0
allow_outside_artifacts=0

usage() {
  cat <<'USAGE'
Usage: package-artifacts.sh --run-dir DIR [--output DIR] [--include-screenshots] [--allow-outside-artifacts]

Create a deterministic directory package for reviewed live-evaluation evidence.
The package includes copied artifact files, SHA-256 checksums, skipped-file
reasons, repository metadata, and Entmoot version output when available.

Options:
  --run-dir DIR                 evaluation run directory to package
  --output DIR                  output package directory override
  --include-screenshots         include screenshots/ files and common image types
  --allow-outside-artifacts     allow --run-dir outside artifacts/evaluation/
  -h, --help                    show this help

Environment:
  ENTMOOTD                      entmootd or wrapper path for version capture
  ENTMOOT_AGENT_WRAPPER         optional wrapper path (default: /data/.entmoot/entmoot)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-dir)
      eval_require_value "$script_name" "$1" "${2:-}"
      run_dir="$2"
      shift 2
      ;;
    --output)
      eval_require_value "$script_name" "$1" "${2:-}"
      output="$2"
      shift 2
      ;;
    --include-screenshots)
      include_screenshots=1
      shift
      ;;
    --allow-outside-artifacts)
      allow_outside_artifacts=1
      shift
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

[[ -n "$run_dir" ]] || eval_usage_error "$script_name" "--run-dir is required"
[[ -d "$run_dir" ]] || eval_die "run directory not found: $run_dir"

repo_root="$(eval_repo_root)"
run_dir_abs="$(cd "$run_dir" && pwd)"
artifacts_root="$(cd "$repo_root/artifacts/evaluation" && pwd)"

case "$run_dir_abs" in
  "$artifacts_root"/*) ;;
  *)
    if (( ! allow_outside_artifacts )); then
      eval_die "run directory must be under artifacts/evaluation/; pass --allow-outside-artifacts to override"
    fi
    ;;
esac

run_name="$(basename "$run_dir_abs")"
safe_run_name="$(eval_safe_component "$run_name")"
if [[ -z "$output" ]]; then
  output="$artifacts_root/packages/$safe_run_name-package"
fi
package_abs="$output"
if [[ "$package_abs" != /* ]]; then
  package_abs="$repo_root/$package_abs"
fi
package_parent="$(dirname "$package_abs")"
package_base="$(basename "$package_abs")"
mkdir -p "$package_parent"
package_parent_abs="$(cd "$package_parent" && pwd)"
package_abs="$package_parent_abs/$package_base"

if [[ -e "$package_abs" ]]; then
  eval_die "output already exists: $package_abs"
fi

eval_require_command git
if command -v sha256sum >/dev/null 2>&1; then
  checksum_cmd=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  checksum_cmd=(shasum -a 256)
else
  eval_die "sha256sum or shasum is required"
fi

mkdir -p "$package_abs/files"
skipped_path="$package_abs/skipped-files.txt"
manifest_path="$package_abs/package-manifest.txt"
: >"$skipped_path"

skip_reason() {
  local rel="$1"
  local abs="$2"
  local lower
  lower="$(printf '%s' "$rel" | tr '[:upper:]' '[:lower:]')"

  if (( ! include_screenshots )); then
    case "$lower" in
      screenshots/*|*/screenshots/*|*.png|*.jpg|*.jpeg|*.gif|*.webp|*.heic)
        printf '%s\n' "screenshots-not-included"
        return 0
        ;;
    esac
  fi

  case "$lower" in
    .env|.env.*|*/.env|*/.env.*|*.env|*.env.*|*invite*|*open-invite*|*descriptor*|*password*|*token*|*secret*|*credential*|*id_rsa*|*id_ed25519*|*.pem|*.key|*ssh*)
      printf '%s\n' "secret-like-name"
      return 0
      ;;
  esac

  if LC_ALL=C grep -I -Eiq \
    'BEGIN (OPENSSH |RSA |EC |DSA )?PRIVATE KEY|Authorization: Bearer [^<]|(^|[?&[:space:]])[A-Z0-9_]*(token|password|secret|api[_-]?key|access[_-]?key|secret[_-]?key|access[_-]?token)[A-Z0-9_]*=|"(token|password|secret|api[_-]?key|access[_-]?key|secret[_-]?key|access[_-]?token)"[[:space:]]*:' \
    "$abs"; then
    printf '%s\n' "secret-like-content"
    return 0
  fi

  return 1
}

copy_count=0
skip_count=0
while IFS= read -r abs_path; do
  rel_path="${abs_path#"$run_dir_abs"/}"
  if reason="$(skip_reason "$rel_path" "$abs_path")"; then
    printf '%s\t%s\n' "$reason" "$rel_path" >>"$skipped_path"
    skip_count=$((skip_count + 1))
    continue
  fi
  mkdir -p "$package_abs/files/$(dirname "$rel_path")"
  cp "$abs_path" "$package_abs/files/$rel_path"
  copy_count=$((copy_count + 1))
done < <(find "$run_dir_abs" -path "$package_abs" -prune -o -type f -print | LC_ALL=C sort)

{
  printf 'package_created_utc=%s\n' "$(eval_timestamp_utc)"
  printf 'repo_root=%s\n' "$repo_root"
  printf 'repo_commit=%s\n' "$(git -C "$repo_root" rev-parse HEAD)"
  printf 'repo_branch=%s\n' "$(git -C "$repo_root" branch --show-current)"
  printf 'run_dir=%s\n' "$run_dir_abs"
  printf 'package_dir=%s\n' "$package_abs"
  printf 'include_screenshots=%s\n' "$include_screenshots"
  printf 'copied_files=%s\n' "$copy_count"
  printf 'skipped_files=%s\n' "$skip_count"
  printf 'script_commit=%s\n' "$(git -C "$repo_root" rev-parse HEAD)"
  printf 'group_ids='
  find "$run_dir_abs" -name run.meta -type f -exec awk '/^group_id=/ {value=substr($0, index($0, "=") + 1); if (value != "") print value}' {} + | LC_ALL=C sort -u | tr '\n' ' '
  printf '\n'
  printf 'node_labels='
  find "$run_dir_abs" -name run.meta -type f -exec awk '/^node_label=/ {value=substr($0, index($0, "=") + 1); if (value != "") print value}' {} + | LC_ALL=C sort -u | tr '\n' ' '
  printf '\n'
} >"$manifest_path"

entmootd_bin="$(eval_resolve_entmootd)"
entmootd_available=0
if [[ "$entmootd_bin" == */* ]]; then
  [[ -x "$entmootd_bin" ]] && entmootd_available=1
elif command -v "$entmootd_bin" >/dev/null 2>&1; then
  entmootd_available=1
fi

if (( entmootd_available )); then
  if "$entmootd_bin" version >"$package_abs/entmoot-version.json" 2>"$package_abs/entmoot-version.stderr"; then
    printf 'entmoot_version_status=0\n' >>"$manifest_path"
  else
    status=$?
    printf 'entmoot_version_status=%s\n' "$status" >>"$manifest_path"
  fi
else
  printf 'entmoot_version_status=unavailable\n' >>"$manifest_path"
  printf 'entmootd unavailable: %s\n' "$entmootd_bin" >"$package_abs/entmoot-version.stderr"
fi

git -C "$repo_root" status --short --branch >"$package_abs/repo-status.txt"

checksums_path="$package_abs/checksums.sha256"
(
  cd "$package_abs"
  while IFS= read -r rel; do
    "${checksum_cmd[@]}" "$rel"
  done < <(find . -type f ! -path "./$(basename "$checksums_path")" | LC_ALL=C sort)
) >"$checksums_path"

echo "package output: $package_abs"
echo "copied files: $copy_count"
echo "skipped files: $skip_count"
echo "checksums: $checksums_path"
