#!/bin/sh
# Run paired performance with fixture cleanup outside any agent or CI session.
set -eu
cleanup() { make broker-down; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
make broker
"${PYTHON:-python3}" scripts/performance.py run "$@"
