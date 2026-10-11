#!/usr/bin/env bash
set -euo pipefail

runner_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
host_dir="$(cd "$runner_dir/.." && pwd)"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT

artifact="$temporary_dir/libissue_tracker_runner.so"
manifest="$temporary_dir/release-manifest.json"
runner_source="$temporary_dir/runner-source"
host_source="$temporary_dir/host-source"
mkdir "$runner_source" "$host_source"
cp "$runner_dir/Cargo.toml" "$runner_source/Cargo.toml"
printf 'runner fixture\n' > "$runner_source/README.md"
printf 'host fixture\n' > "$host_source/README.md"
for source_dir in "$runner_source" "$host_source"; do
  git -C "$source_dir" init --quiet
  git -C "$source_dir" add .
  git -C "$source_dir" -c user.name=fixture -c user.email=fixture@example.invalid \
    commit --quiet -m initial
done
printf 'release artifact fixture\n' > "$artifact"
"$runner_dir/scripts/generate-release-manifest.sh" \
  --artifact "$artifact" \
  --output "$manifest" \
  --host-dir "$host_source" \
  --runner-dir "$runner_source"
"$runner_dir/scripts/verify-release-manifest.sh" \
  --artifact "$artifact" \
  --manifest "$manifest" \
  --host-dir "$host_source" \
  --runner-dir "$runner_source"

printf 'dirty\n' > "$runner_source/dirty-marker"
if "$runner_dir/scripts/verify-release-manifest.sh" \
  --artifact "$artifact" \
  --manifest "$manifest" \
  --host-dir "$host_source" \
  --runner-dir "$runner_source"; then
  echo "dirty source worktree was accepted" >&2
  exit 1
fi
git -C "$runner_source" clean --force --quiet

printf 'tampered\n' >> "$artifact"
if "$runner_dir/scripts/verify-release-manifest.sh" \
  --artifact "$artifact" \
  --manifest "$manifest" \
  --host-dir "$host_source" \
  --runner-dir "$runner_source"; then
  echo "tampered artifact was accepted" >&2
  exit 1
fi
