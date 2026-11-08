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
    auth)
        run_id=${KAFERRY_BROKER_RUN_ID:?fixture run ID is required}
        brokers='kafka-secure-1 kafka-secure-2 kafka-secure-3'
        [ "$project" = kaferry-qualification ] || fail 'storage requires the disposable authentication fixture project'
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

deletion_diagnostics() {
    printf 'cleanup census: metadata only; 2s deadline, 65536-byte limit\n' >&2
    if ! command -v timeout >/dev/null || ! command -v head >/dev/null ||
       ! command -v find >/dev/null || [ ! -x /bin/bash ]; then
        echo 'cleanup census unavailable: required fixture tools are missing' >&2
        return
    fi
    if ! census=$(mktemp); then
        echo 'cleanup census unavailable: cannot create temporary output' >&2
        return
    fi
    if timeout -k 1 2 /bin/bash -o pipefail -c '
        LC_ALL=C find -P "$1" -maxdepth 4 -printf "type=%y device=%D inode=%i bytes=%s links=%n path=%p\\n" 2>&1 |
            head -c 65537
    ' census "$1" > "$census" 2>&1; then
        census_status=0
    else
        census_status=$?
    fi
    census_bytes=$(wc -c < "$census")
    head -c 65536 "$census" >&2 || true
    printf '\ncleanup census exit=%s\n' "$census_status" >&2
    case "$census_status" in 124|137) echo 'cleanup census timed out; metadata is incomplete' >&2 ;; esac
    [ "$census_bytes" -le 65536 ] || echo 'cleanup census truncated; metadata is incomplete' >&2
    [ "$census_status" -eq 0 ] || echo 'cleanup census failed; metadata is incomplete' >&2
    rm -f -- "$census" || echo 'cleanup census temporary output could not be removed' >&2
    return 0
}

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
        if [ "$layout" != replicated ]; then
            for broker in $brokers; do
                [ ! -L "$run/$broker" ] || fail 'refusing a symbolic link in fixture storage'
            done
        fi
        # Keep ownership proof if a recursive deletion stops partway through.
        for path in "$run"/* "$run"/.[!.]* "$run"/..?*; do
            if [ "$path" != "$run/.kaferry-owned" ]; then
                if rm -rf -- "$path"; then :; else
                    deletion_status=$?
                    deletion_diagnostics "$path" || echo 'cleanup census unavailable' >&2
                    exit "$deletion_status"
                fi
            fi
        done
        if rm -- "$run/.kaferry-owned"; then :; else
            deletion_status=$?
            deletion_diagnostics "$run" || echo 'cleanup census unavailable' >&2
            exit "$deletion_status"
        fi
        if rmdir -- "$run"; then :; else
            deletion_status=$?
            deletion_diagnostics "$run" || echo 'cleanup census unavailable' >&2
            if [ ! -d "$run" ] || [ -L "$run" ]; then
                echo 'fixture storage disappeared during cleanup' >&2
                exit "$deletion_status"
            fi
            if ! (set -C; printf '%s\n' "$marker" > "$run/.kaferry-owned"); then
                echo 'cannot restore fixture storage ownership marker' >&2
                exit "$deletion_status"
            fi
            echo 'fixture storage directory removal failed; ownership marker restored' >&2
            exit "$deletion_status"
        fi
        rmdir "$project_root" "$namespace" 2>/dev/null || true
        ;;
    *) fail 'use capacity, prepare or clean' ;;
esac
