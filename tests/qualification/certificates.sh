#!/bin/sh
set -eu
cert_dir=${1:?certificate output directory required}
mkdir -p "$cert_dir"
umask 077
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -sha256 \
  -keyout "$cert_dir/ca.key" -out "$cert_dir/ca.pem" -subj /CN=kafka-fwd2-test-ca >/dev/null 2>&1
openssl req -new -newkey rsa:2048 -nodes \
  -keyout "$cert_dir/broker.key" -out "$cert_dir/broker.csr" -subj /CN=localhost >/dev/null 2>&1
cat > "$cert_dir/extensions.txt" <<'EOF'
subjectAltName=DNS:localhost,DNS:kafka-secure-1,DNS:kafka-secure-2,DNS:kafka-secure-3,IP:127.0.0.1
extendedKeyUsage=serverAuth
EOF
openssl x509 -req -days 2 -sha256 -in "$cert_dir/broker.csr" \
  -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca.key" -CAserial "$cert_dir/ca.srl" -CAcreateserial \
  -extfile "$cert_dir/extensions.txt" -out "$cert_dir/broker.pem" >/dev/null 2>&1
openssl pkcs12 -export -name kafka -in "$cert_dir/broker.pem" \
  -inkey "$cert_dir/broker.key" -certfile "$cert_dir/ca.pem" \
  -passout pass:fixture-keystore-secret -out "$cert_dir/broker.p12"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -sha256 \
  -keyout "$cert_dir/wrong-ca.key" -out "$cert_dir/wrong-ca.pem" -subj /CN=kafka-fwd2-wrong-ca >/dev/null 2>&1
chmod 755 "$cert_dir"
chmod 644 "$cert_dir/broker.p12" "$cert_dir/ca.pem" "$cert_dir/wrong-ca.pem"
