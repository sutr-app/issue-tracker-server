#!/usr/bin/env bash

release_manifest_usage() {
  echo "usage: $1 --artifact FILE --host-dir DIR [--runner-dir DIR] --output FILE" >&2
}

release_manifest_parse_args() {
  artifact=""
  host_dir=""
  output=""
  runner_dir="$(cd "$(dirname "${BASH_SOURCE[1]}")/.." && pwd)"
  while (($#)); do
    case "$1" in
      --artifact) artifact="$2"; shift 2 ;;
      --host-dir) host_dir="$2"; shift 2 ;;
      --runner-dir) runner_dir="$2"; shift 2 ;;
      --output|--manifest) output="$2"; shift 2 ;;
      *) return 2 ;;
    esac
  done
  [[ -n "$artifact" && -n "$host_dir" && -n "$output" ]] || return 2
  [[ -f "$artifact" ]] || { echo "artifact is not a regular file: $artifact" >&2; return 1; }
  git -C "$runner_dir" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
    echo "runner directory is not a git repository: $runner_dir" >&2
    return 1
  }
  git -C "$host_dir" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
    echo "host directory is not a git repository: $host_dir" >&2
    return 1
  }
}

release_manifest_content() {
  local taskchampion_version artifact_name artifact_sha256 runner_revision host_revision
  taskchampion_version="$(sed -nE 's/^taskchampion = \{ version = "=([^"]+)".*/\1/p' "$runner_dir/Cargo.toml")"
  [[ -n "$taskchampion_version" ]] || { echo "TaskChampion must be pinned in Cargo.toml" >&2; return 1; }
  artifact_name="$(basename "$artifact")"
  [[ "$artifact_name" != *'"'* && "$artifact_name" != *\\* ]] || { echo "artifact filename is not JSON-safe" >&2; return 1; }
  artifact_sha256="$(sha256sum "$artifact" | awk '{print $1}')"
  runner_revision="$(git -C "$runner_dir" rev-parse HEAD)"
  host_revision="$(git -C "$host_dir" rev-parse HEAD)"
  printf '{"schema_version":1,"artifact":{"filename":"%s","sha256":"%s"},"source":{"runner_revision":"%s","jobworkerp_host_revision":"%s"},"dependencies":{"taskchampion_version":"%s"}}\n' \
    "$artifact_name" "$artifact_sha256" "$runner_revision" "$host_revision" "$taskchampion_version"
}

release_manifest_require_clean_sources() {
  for source_dir in "$runner_dir" "$host_dir"; do
    [[ -z "$(git -C "$source_dir" status --porcelain)" ]] || {
      echo "source worktree is not clean: $source_dir" >&2
      return 1
    }
  done
}
