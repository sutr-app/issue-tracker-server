#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/release-manifest-lib.sh"
release_manifest_parse_args "$@" || { release_manifest_usage "$0"; exit 2; }
release_manifest_require_clean_sources
manifest="$output"
[[ -f "$manifest" ]] || { echo "release manifest is missing: $manifest" >&2; exit 1; }

expected="$(mktemp)"
trap 'rm -f "$expected"' EXIT
release_manifest_content > "$expected"
cmp --silent "$expected" "$manifest" || {
  echo "release manifest does not match artifact checksum, source revisions, or TaskChampion version" >&2
  exit 1
}
