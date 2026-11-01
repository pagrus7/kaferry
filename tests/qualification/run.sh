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
  cleanup_status=0
  compose down -v || cleanup_status=$?
  rm -rf "$cert_dir"
  return "$cleanup_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
sh tests/qualification/certificates.sh "$cert_dir"
case "${KAFERRY_AUTH_IMAGE:-apache/kafka:3.9.2}" in
  *kafka:3.*) export KAFERRY_OAUTH_VALIDATOR=org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler ;;
  *) export KAFERRY_OAUTH_VALIDATOR=org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler ;;
esac
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
CGO_ENABLED=1 "${GO:-go}" test -count=1 -timeout=10m -race -tags=integration,qualification \
  -run 'TestSaslTls|TestReplicatedCluster|TestMutualTls|TestServerTls' "${KAFERRY_TEST_OUTPUT:--v}" ./tests/compat
