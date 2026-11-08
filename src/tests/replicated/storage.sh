#!/bin/sh
set -eu
umask 077

fail() { echo "$*" >&2; exit 1; }
root=${KAFERRY_STORAGE_ROOT:-/storage}
project=${KAFERRY_FIXTURE_PROJECT:?fixture project is required}
layout=${KAFERRY_STORAGE_LAYOUT:-replicated}
case "$layout" in
    replicated)
        run_id=${KAFERRY_REPLICATED_RUN_ID:?fixture run ID is required}
        brokers='source-1 source-2 source-3 destination-1 destination-2 destination-3'
        case "$project" in
            kaferry-replicated|kaferry-lab-*) ;;
            *) fail 'storage requires a disposable replicated fixture project' ;;
        esac
        ;;
    paired)
        run_id=${KAFERRY_BROKER_RUN_ID:?fixture run ID is required}
        brokers='kafka kafka-dr'
        case "$project" in
            kaferry-compat|kaferry-endurance|kaferry-lab-*) ;;
            *) fail 'storage requires a disposable paired fixture project' ;;
        esac
        ;;
    *) fail 'invalid fixture storage layout' ;;
esac
case "$project" in *[!a-z0-9_-]*) fail 'invalid fixture project' ;; esac
case "$run_id" in *[!a-f0-9]*) fail 'invalid fixture run ID' ;; esac
[ "${#run_id}" -eq 32 ] || fail 'fixture run ID must contain 32 hexadecimal characters'

namespace=$root/kaferry-$layout
project_root=$namespace/$project
run=$project_root/$run_id
for path in "$root" "$namespace" "$project_root" "$run"; do
    [ ! -L "$path" ] || fail 'refusing a symbolic link in fixture storage'
done
marker=$(printf 'kaferry-%s-v1\n%s\n%s' "$layout" "$project" "$run_id")

case "${1:-}" in
    capacity)
        df -Pk "$root" | awk 'END {print $4}'
        ;;
    prepare)
        mkdir -p -m 755 "$namespace"
        mkdir -p -m 755 "$project_root"
        mkdir -m 755 "$run"
        printf '%s\n' "$marker" > "$run/.kaferry-owned"
        for broker in $brokers; do
            mkdir -m 700 "$run/$broker"
            chown "${KAFERRY_STORAGE_UID:-1000}:${KAFERRY_STORAGE_GID:-1000}" "$run/$broker"
        done
        ;;
    clean)
        [ -e "$run" ] || exit 0
        [ -f "$run/.kaferry-owned" ] && [ ! -L "$run/.kaferry-owned" ] || fail 'refusing unowned fixture storage'
        [ "$(cat "$run/.kaferry-owned")" = "$marker" ] || fail 'fixture storage ownership does not match this run'
        if [ "$layout" = paired ]; then
            for broker in $brokers; do
                [ ! -L "$run/$broker" ] || fail 'refusing a symbolic link in fixture storage'
            done
        fi
        # Keep ownership proof if a recursive deletion stops partway through.
        for path in "$run"/* "$run"/.[!.]* "$run"/..?*; do
            [ "$path" = "$run/.kaferry-owned" ] || rm -rf -- "$path"
        done
        rm -- "$run/.kaferry-owned"
        if ! rmdir -- "$run"; then
            [ -d "$run" ] && [ ! -L "$run" ] || fail 'fixture storage disappeared during cleanup'
            (set -C; printf '%s\n' "$marker" > "$run/.kaferry-owned") || fail 'cannot restore fixture storage ownership marker'
            fail 'fixture storage directory removal failed; ownership marker restored'
        fi
        rmdir "$project_root" "$namespace" 2>/dev/null || true
        ;;
    *) fail 'use capacity, prepare or clean' ;;
esac
