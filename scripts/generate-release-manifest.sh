#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/release-manifest-lib.sh"
release_manifest_parse_args "$@" || { release_manifest_usage "$0"; exit 2; }
release_manifest_require_clean_sources

temporary_output="$(mktemp "$(dirname "$output")/.release-manifest.XXXXXX")"
trap 'rm -f "$temporary_output"' EXIT
release_manifest_content > "$temporary_output"
mv "$temporary_output" "$output"
