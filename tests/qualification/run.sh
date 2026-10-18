#!/bin/sh
set -eu
mkdir -p .cache/qualification
cert_dir=$(mktemp -d "$PWD/.cache/qualification/run.XXXXXX")
export KAFERRY_QUAL_CERT_DIR="$cert_dir"
compose() {
  docker compose -p kaferry-qualification -f tests/qualification/compose.yaml "$@"
}
cleanup() {
  if [ -n "${KAFERRY_REPORT_DIR:-}" ]; then
    mkdir -p "$KAFERRY_REPORT_DIR"
    compose logs --no-color > "$KAFERRY_REPORT_DIR/security-brokers.log" || true
  fi
  compose down -v
  rm -rf "$cert_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
sh tests/qualification/certificates.sh "$cert_dir"
compose up -d --wait --wait-timeout 90
export KAFERRY_QUAL_BROKERS=127.0.0.1:41092,127.0.0.1:42092,127.0.0.1:43092
export KAFERRY_QUAL_CA="$cert_dir/ca.pem"
export KAFERRY_QUAL_WRONG_CA="$cert_dir/wrong-ca.pem"
CGO_ENABLED=1 "${GO:-go}" test -count=1 -timeout=5m -race -tags=integration,qualification \
  -run 'TestSaslTls|TestReplicatedCluster' -v ./tests/compat
