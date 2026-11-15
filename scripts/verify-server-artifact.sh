#!/usr/bin/env bash
# Usage: verify-server-artifact.sh <binary> <linux-x86_64|linux-aarch64|darwin-arm64> <expected-version>
#
# Smoke check of a built issue-tracker-server binary, run by the public
# release workflow on the runner that built it, before anything is published.
# It checks what makes a release unusable on the user's machine: file format
# and architecture, that Linux builds are fully static (no libc floor), that
# macOS builds link only system libraries, carry a valid signature and do not
# demand a newer macOS than MACOSX_DEPLOYMENT_TARGET, and that the binary runs
# and reports the release version.
#
# Exit status: 1 when the artifact fails a check, 2 on usage errors.
# Kept compatible with the bash 3.2 shipped with macOS.
set -euo pipefail

binary=${1-}
platform=${2-}
expected_version=${3-}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

usage_error() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 2
}

# True when dotted version $1 is greater than $2. Compared field by field in
# awk because `sort -V` is not available on every macOS release.
version_gt() {
  awk -v a="$1" -v b="$2" 'BEGIN {
    na = split(a, x, "."); nb = split(b, y, "."); n = (na > nb) ? na : nb
    for (i = 1; i <= n; i++) {
      if ((x[i] + 0) > (y[i] + 0)) exit 0
      if ((x[i] + 0) < (y[i] + 0)) exit 1
    }
    exit 1
  }'
}

check_static_elf() {
  local kind=$1 arch=$2
  [[ "${kind}" == *"ELF 64-bit LSB"*"${arch}"* ]] || die "not a ${arch} ELF executable"
  [[ "${kind}" == *"static-pie linked"* || "${kind}" == *"statically linked"* ]] \
    || die "not statically linked; the release must not depend on the host libc"
}

[[ -n "${binary}" && -n "${platform}" && -n "${expected_version}" ]] \
  || usage_error "usage: $0 <binary> <linux-x86_64|linux-aarch64|darwin-arm64> <expected-version>"
[[ -f "${binary}" ]] || usage_error "no such file: ${binary}"
[[ -x "${binary}" ]] || usage_error "not executable: ${binary}"

kind=$(file -b "${binary}")
echo "file: ${kind}"

case "${platform}" in
  linux-x86_64)
    check_static_elf "${kind}" x86-64
    ;;
  linux-aarch64)
    check_static_elf "${kind}" "ARM aarch64"
    ;;
  darwin-arm64)
    [[ -n "${MACOSX_DEPLOYMENT_TARGET-}" ]] || usage_error "MACOSX_DEPLOYMENT_TARGET must be set for ${platform}"
    [[ "${kind}" == *"Mach-O 64-bit executable arm64"* ]] || die "not an arm64 Mach-O executable"
    echo "dynamic dependencies:"
    libraries=$(otool -L "${binary}" | tail -n +2 | awk '{ print $1 }')
    for library in ${libraries}; do
      echo "  ${library}"
      case "${library}" in
        /usr/lib/* | /System/Library/*) ;;
        *) die "dynamic dependency outside the macOS system: ${library}" ;;
      esac
    done
    # Captured and fed without a pipe: an awk exiting at the first match
    # would SIGPIPE the writer, which pipefail turns into a silent failure.
    load_commands=$(otool -l "${binary}")
    minos=$(awk '$1 == "minos" { print $2; exit }' <<<"${load_commands}")
    echo "minimum macOS: ${minos:-unknown}"
    [[ -n "${minos}" ]] || die "no LC_BUILD_VERSION minos found"
    if version_gt "${minos}" "${MACOSX_DEPLOYMENT_TARGET}"; then
      die "requires macOS ${minos}, above MACOSX_DEPLOYMENT_TARGET ${MACOSX_DEPLOYMENT_TARGET}"
    fi
    codesign -v --verbose=2 "${binary}" || die "code signature is invalid"
    ;;
  *)
    usage_error "unknown platform: ${platform}"
    ;;
esac

reported=$("${binary}" --version) || die "--version failed"
echo "version: ${reported}"
[[ "${reported}" == "issue-tracker-server ${expected_version}" ]] \
  || die "reports '${reported}', expected issue-tracker-server ${expected_version}"

echo "OK: ${binary} (${platform})"
