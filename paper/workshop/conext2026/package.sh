#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
pkg_name="entmoot-conext2026-workshop-package-$stamp"
pkg_dir="$repo_root/paper/workshop/conext2026/dist/$pkg_name"

mkdir -p "$pkg_dir"/{paper,generated,workshop}

cp "$repo_root/paper/entmoot-paper.pdf" "$pkg_dir/paper/"
cp "$repo_root/paper/main.tex" "$pkg_dir/paper/"
cp "$repo_root/paper/refs.bib" "$pkg_dir/paper/"
if [[ -f "$repo_root/paper/main.bbl" ]]; then
  cp "$repo_root/paper/main.bbl" "$pkg_dir/paper/"
fi

cp -R "$repo_root/paper/generated/evaluation" "$pkg_dir/generated/"
cp -R "$repo_root/paper/generated/benchmarks" "$pkg_dir/generated/"
cp -R "$repo_root/paper/generated/security" "$pkg_dir/generated/"

cp "$repo_root/paper/workshop/conext2026/README.md" "$pkg_dir/workshop/"
cp "$repo_root/paper/workshop/conext2026/VENUE-STATUS.md" "$pkg_dir/workshop/"
cp "$repo_root/paper/workshop/conext2026/SUBMISSION-HANDOFF.md" "$pkg_dir/workshop/"
cp "$repo_root/paper/workshop/conext2026/SUBMISSION-SUMMARY.md" "$pkg_dir/workshop/"

{
  printf 'package_created_utc=%s\n' "$stamp"
  printf 'repo_commit=%s\n' "$(git -C "$repo_root" rev-parse HEAD)"
  printf 'repo_branch=%s\n' "$(git -C "$repo_root" branch --show-current)"
  printf 'paper_pdf_sha256='
  sha256sum "$repo_root/paper/entmoot-paper.pdf" | awk '{print $1}'
  printf 'notes=%s\n' 'No raw evaluation artifacts are included.'
} > "$pkg_dir/PACKAGE-MANIFEST.txt"

(
  cd "$pkg_dir"
  find . -type f ! -name CHECKSUMS.sha256 -print0 | sort -z | xargs -0 sha256sum
) > "$pkg_dir/CHECKSUMS.sha256"

tarball="$pkg_dir.tar.gz"
tar -C "$(dirname "$pkg_dir")" -czf "$tarball" "$(basename "$pkg_dir")"

printf 'package_dir=%s\n' "$pkg_dir"
printf 'tarball=%s\n' "$tarball"
printf 'checksums=%s\n' "$pkg_dir/CHECKSUMS.sha256"
