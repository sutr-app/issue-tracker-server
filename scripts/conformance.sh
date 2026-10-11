#!/usr/bin/env bash
set -euo pipefail

manifest_path="${1:-Cargo.toml}"
expected_version="${TASKCHAMPION_VERSION:-3.1.0}"

grep -q "version = \"=${expected_version}\"" "$(dirname "$manifest_path")/Cargo.toml"
"$(dirname "$manifest_path")/tests/release_gates.sh"
cargo test --offline --locked --manifest-path "$manifest_path" -- --test-threads=1
cargo fmt --manifest-path "$manifest_path" -- --check
cargo clippy --offline --locked --manifest-path "$manifest_path" --all-targets -- -D warnings
