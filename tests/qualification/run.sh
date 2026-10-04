#!/bin/sh
set -eu
mkdir -p .cache/qualification
cert_dir=$(mktemp -d "$PWD/.cache/qualification/run.XXXXXX")
export KFWD_QUAL_CERT_DIR="$cert_dir"
compose() {
  docker compose -p kafka-fwd2-qualification -f tests/qualification/compose.yaml "$@"
}
cleanup() {
  compose down -v
  rm -rf "$cert_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
sh tests/qualification/certificates.sh "$cert_dir"
compose up -d --wait --wait-timeout 90
export KFWD_QUAL_BROKERS=127.0.0.1:41092,127.0.0.1:42092,127.0.0.1:43092
export KFWD_QUAL_CA="$cert_dir/ca.pem"
export KFWD_QUAL_WRONG_CA="$cert_dir/wrong-ca.pem"
"${GO:-go}" test -race -tags=integration,qualification \
  -run 'TestSaslTls|TestReplicatedCluster' -v ./tests/compat
