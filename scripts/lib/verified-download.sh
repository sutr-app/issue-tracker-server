#!/usr/bin/env bash
# Sourced by the install-*.sh scripts, which fetch pinned tool releases
# without sudo or a package manager. Kept compatible with the bash 3.2
# shipped with macOS.

# verified_download <url> <sha256> <file>: downloads <url> to <file> and exits
# with status 1 unless the download succeeds and its SHA-256 is <sha256>, so
# nothing unverified is ever extracted or run.
verified_download() {
  local url=$1 expected=$2 file=$3 actual
  curl -fsSL -o "${file}" "${url}" || {
    printf 'ERROR: download failed: %s\n' "${url}" >&2
    exit 1
  }
  if command -v sha256sum >/dev/null; then
    actual=$(sha256sum "${file}" | awk '{ print $1 }')
  else
    actual=$(shasum -a 256 "${file}" | awk '{ print $1 }')
  fi
  [[ "${actual}" == "${expected}" ]] || {
    printf 'ERROR: checksum mismatch for %s: expected %s, got %s\n' "$(basename "${file}")" "${expected}" "${actual}" >&2
    exit 1
  }
}
