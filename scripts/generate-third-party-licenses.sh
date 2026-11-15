#!/usr/bin/env bash
# Usage: generate-third-party-licenses.sh <output-file>
#
# Writes the notices (copyright and license texts of every third-party crate
# linked into issue-tracker-server) that ship with the release archives, and
# doubles as the license policy check: it fails when a dependency is under a
# license not accepted in about.toml, or when a crate's license cannot be
# determined, which cargo-about itself only warns about. Nothing is written
# when it fails.
#
# Uses CARGO_ABOUT (as exported by install-cargo-about.sh) or the
# `cargo about` subcommand. ABOUT_CONFIG and ABOUT_MANIFEST override
# about.toml and the server crate manifest (tests).
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output=${1-}
config=${ABOUT_CONFIG:-${ROOT}/about.toml}
manifest=${ABOUT_MANIFEST:-${ROOT}/crates/issue-tracker-front/Cargo.toml}

[[ -n "${output}" ]] || {
  printf 'ERROR: usage: %s <output-file>\n' "$0" >&2
  exit 2
}

if [[ -n "${CARGO_ABOUT-}" ]]; then
  about=("${CARGO_ABOUT}")
else
  about=(cargo about)
fi

work=$(mktemp -d)
trap 'rm -rf "${work}"' EXIT

"${about[@]}" generate --locked --fail --config "${config}" --manifest-path "${manifest}" \
  --output-file "${work}/notices.txt" "${ROOT}/about.hbs"

# about.hbs marks crates with an undetermined license at the top.
unknown=$(sed -n 's/^UNKNOWN-LICENSE: //p' "${work}/notices.txt")
[[ -z "${unknown}" ]] || {
  printf 'ERROR: license could not be determined (clarify it in about.toml) for:\n%s\n' "${unknown}" >&2
  exit 1
}

mkdir -p "$(dirname "${output}")"
mv "${work}/notices.txt" "${output}"
echo "wrote ${output}"
