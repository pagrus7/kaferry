#!/bin/sh
set -eu
cert_dir=${1:?certificate output directory required}
mkdir -p "$cert_dir"
umask 077
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -sha256 \
  -keyout "$cert_dir/ca.key" -out "$cert_dir/ca.pem" -subj /CN=kaferry-test-ca >/dev/null 2>&1
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
  -keyout "$cert_dir/wrong-ca.key" -out "$cert_dir/wrong-ca.pem" -subj /CN=kaferry-wrong-ca >/dev/null 2>&1
chmod 755 "$cert_dir"
chmod 644 "$cert_dir/broker.p12" "$cert_dir/ca.pem" "$cert_dir/wrong-ca.pem"
# Broker-side JWT signature validation and a client certificate for mTLS.
openssl req -new -newkey rsa:2048 -nodes -keyout "$cert_dir/client.key" \
  -out "$cert_dir/client.csr" -subj /CN=copier >/dev/null 2>&1
printf 'extendedKeyUsage=clientAuth\n' > "$cert_dir/client-extensions.txt"
openssl x509 -req -days 2 -sha256 -in "$cert_dir/client.csr" \
  -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca.key" -CAserial "$cert_dir/ca.srl" \
  -extfile "$cert_dir/client-extensions.txt" -out "$cert_dir/client.pem" >/dev/null 2>&1
for identity in 1 2; do
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
    -out "$cert_dir/oauth-$identity.key" >/dev/null 2>&1
  openssl rsa -in "$cert_dir/oauth-$identity.key" -noout -modulus > "$cert_dir/oauth-$identity.modulus" 2>/dev/null
done
mkdir -p "$cert_dir/jwks"
chmod 755 "$cert_dir/jwks"
python3 - "$cert_dir" <<'PY'
import base64
import json
from pathlib import Path
import sys
root = Path(sys.argv[1])
keys = []
for identity in (1, 2):
    n = int((root / f'oauth-{identity}.modulus').read_text().strip().split('=')[1], 16)
    keys.append({'kty': 'RSA', 'alg': 'RS256', 'use': 'sig', 'kid': f'qualification-{identity}',
                 'n': base64.urlsafe_b64encode(n.to_bytes((n.bit_length() + 7) // 8, 'big')).decode().rstrip('='),
                 'e': 'AQAB'})
(root / 'jwks/keys.json').write_text(json.dumps({'keys': keys[:1]}))
(root / 'jwks-rotated.json').write_text(json.dumps({'keys': keys}))
PY
chmod 644 "$cert_dir/jwks/keys.json" "$cert_dir/jwks-rotated.json"
