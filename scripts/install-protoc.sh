#!/usr/bin/env bash
# Usage: install-protoc.sh <dest-dir>
#
# Installs a pinned protoc release into <dest-dir> after verifying its
# SHA-256, without sudo or a package manager, so every runner compiles the
# protobuf crates with the same, publicly visible compiler version. Under
# GitHub Actions it also exports PROTOC for prost-build.
#
# Kept compatible with the bash 3.2 shipped with macOS.
set -euo pipefail

# shellcheck source=scripts/lib/verified-download.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/verified-download.sh"

PROTOC_VERSION=36.2

dest=${1-}
base_url=${PROTOC_BASE_URL:-https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}}
platform=${PROTOC_PLATFORM:-$(uname -s)-$(uname -m)}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

[[ -n "${dest}" ]] || die "usage: $0 <dest-dir>"

case "${platform}" in
  Linux-x86_64)
    asset=protoc-${PROTOC_VERSION}-linux-x86_64.zip
    sha256=121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278
    ;;
  Linux-aarch64)
    asset=protoc-${PROTOC_VERSION}-linux-aarch_64.zip
    sha256=8b8f18bd2b30346efbc698dd5a73dd7c805f3ef8380f6dfc95c768f3f1852f6a
    ;;
  Darwin-arm64)
    asset=protoc-${PROTOC_VERSION}-osx-aarch_64.zip
    sha256=9cd98a532c5c5e0c4161314de0225de27e4c8a323917b6ea7b1b714d3ae23466
    ;;
  *)
    die "no pinned protoc for platform ${platform}"
    ;;
esac

work=$(mktemp -d)
trap 'rm -rf "${work}"' EXIT

verified_download "${base_url}/${asset}" "${sha256}" "${work}/${asset}"

mkdir -p "${dest}"
unzip -q -o "${work}/${asset}" -d "${dest}"
"${dest}/bin/protoc" --version

if [[ -n "${GITHUB_ENV-}" ]]; then
  echo "PROTOC=${dest}/bin/protoc" >>"${GITHUB_ENV}"
fi
