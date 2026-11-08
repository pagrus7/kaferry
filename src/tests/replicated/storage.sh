#!/bin/sh
set -eu
umask 077

fail() { echo "$*" >&2; exit 1; }
root=${KAFERRY_STORAGE_ROOT:-/storage}
project=${KAFERRY_FIXTURE_PROJECT:?fixture project is required}
run_id=${KAFERRY_REPLICATED_RUN_ID:?fixture run ID is required}
case "$project" in
    kaferry-replicated|kaferry-lab-*) ;;
    *) fail 'storage requires a disposable replicated fixture project' ;;
esac
case "$project" in *[!a-z0-9_-]*) fail 'invalid fixture project' ;; esac
case "$run_id" in *[!a-f0-9]*) fail 'invalid fixture run ID' ;; esac
[ "${#run_id}" -eq 32 ] || fail 'fixture run ID must contain 32 hexadecimal characters'

namespace=$root/kaferry-replicated
project_root=$namespace/$project
run=$project_root/$run_id
for path in "$root" "$namespace" "$project_root" "$run"; do
    [ ! -L "$path" ] || fail 'refusing a symbolic link in fixture storage'
done
marker=$(printf 'kaferry-replicated-v1\n%s\n%s' "$project" "$run_id")

case "${1:-}" in
    capacity)
        df -Pk "$root" | awk 'END {print $4}'
        ;;
    prepare)
        mkdir -p -m 755 "$namespace"
        mkdir -p -m 755 "$project_root"
        mkdir -m 755 "$run"
        printf '%s\n' "$marker" > "$run/.kaferry-owned"
        for broker in source-1 source-2 source-3 destination-1 destination-2 destination-3; do
            mkdir -m 700 "$run/$broker"
            chown "${KAFERRY_STORAGE_UID:-1000}:${KAFERRY_STORAGE_GID:-1000}" "$run/$broker"
        done
        ;;
    clean)
        [ -e "$run" ] || exit 0
        [ -f "$run/.kaferry-owned" ] && [ ! -L "$run/.kaferry-owned" ] || fail 'refusing unowned fixture storage'
        [ "$(cat "$run/.kaferry-owned")" = "$marker" ] || fail 'fixture storage ownership does not match this run'
        rm -rf -- "$run"
        rmdir "$project_root" "$namespace" 2>/dev/null || true
        ;;
    *) fail 'use capacity, prepare or clean' ;;
esac
