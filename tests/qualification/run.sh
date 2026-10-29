#!/bin/sh
set -eu
mkdir -p .cache/qualification
cert_dir=$(mktemp -d "$PWD/.cache/qualification/run.XXXXXX")
export KAFERRY_QUAL_CERT_DIR="$cert_dir"
compose() {
  docker compose -p kaferry-qualification -f tests/qualification/compose.yaml -f tests/qualification/compose.supervised.yaml "$@"
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
compose up -d --build --wait --wait-timeout 90
if [ -n "${KAFERRY_REPORT_DIR:-}" ]; then
  mkdir -p "$KAFERRY_REPORT_DIR"
  compose images --format json > "$KAFERRY_REPORT_DIR/qualification.broker-images.json"
fi
export KAFERRY_QUAL_BROKERS=127.0.0.1:41092,127.0.0.1:42092,127.0.0.1:43092
export KAFERRY_QUAL_CA="$cert_dir/ca.pem"
export KAFERRY_QUAL_WRONG_CA="$cert_dir/wrong-ca.pem"
export KAFERRY_QUAL_KRB_CONFIG="$cert_dir/client-krb5.conf"
export KAFERRY_QUAL_KEYTAB="$cert_dir/copier.keytab"
CGO_ENABLED=1 "${GO:-go}" test -count=1 -timeout=5m -race -tags=integration,qualification \
  -run 'TestSaslTls|TestReplicatedCluster' "${KAFERRY_TEST_OUTPUT:--v}" ./tests/compat

export KAFERRY_QUAL_FAILOVER_CONTAINERS=kaferry-qualification-kafka-secure-1-1,kaferry-qualification-kafka-secure-2-1,kaferry-qualification-kafka-secure-3-1
export CGO_ENABLED=1
python3 scripts/ci.py secured-transactions "${GO:-go}" test -count=1 -timeout=5m -race -json -run '^TestRealSecuredTransactionsAndCaptureCoordinator$' ./internal/app
python3 scripts/ci.py verify-tests secured-transactions TestRealSecuredTransactionsAndCaptureCoordinator
for stage in prepared-plain prepared-gssapi; do
  test_name=TestRealPreparedCommitSurvivesBrokerRestart
  if [ "$stage" = prepared-gssapi ]; then test_name=TestRealGSSAPIPreparedCommitSurvivesBrokerRestart; fi
  python3 scripts/ci.py "$stage" "${GO:-go}" test -count=1 -timeout=5m -race -json -run "^$test_name$" ./internal/app
  python3 scripts/ci.py verify-tests "$stage" \
    "$test_name/leader/TOPIC_to_TOPIC" "$test_name/leader/TOPIC_to_ARCHIVE" "$test_name/leader/ARCHIVE_to_TOPIC" \
    "$test_name/group-coordinator/TOPIC_to_TOPIC" "$test_name/group-coordinator/TOPIC_to_ARCHIVE" \
    "$test_name/transaction-coordinator/TOPIC_to_TOPIC" "$test_name/transaction-coordinator/TOPIC_to_ARCHIVE" "$test_name/transaction-coordinator/ARCHIVE_to_TOPIC"
done
