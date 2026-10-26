#!/bin/sh
set -eu
cat > /etc/krb5.conf <<'EOF'
[libdefaults]
 default_realm = KAFERRY.TEST
 dns_lookup_kdc = false
 udp_preference_limit = 1
[realms]
 KAFERRY.TEST = {
  kdc = kdc:88
 }
EOF
cp /etc/krb5.conf /mnt/certs/broker-krb5.conf
sed 's/kdc:88/127.0.0.1:44088/' /etc/krb5.conf > /mnt/certs/client-krb5.conf
kdb5_util create -s -P fixture-master-secret
kadmin.local -q 'addprinc -randkey copier@KAFERRY.TEST'
kadmin.local -q 'addprinc -randkey kafka/127.0.0.1@KAFERRY.TEST'
kadmin.local -q 'ktadd -k /mnt/certs/copier.keytab copier@KAFERRY.TEST'
kadmin.local -q 'ktadd -k /mnt/certs/broker.keytab kafka/127.0.0.1@KAFERRY.TEST'
chmod 644 /mnt/certs/broker-krb5.conf /mnt/certs/client-krb5.conf /mnt/certs/copier.keytab /mnt/certs/broker.keytab
exec krb5kdc -n
