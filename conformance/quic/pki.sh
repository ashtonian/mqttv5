#!/bin/sh
# Builds the PKI for EMQX's QUIC listener into /pki once: a root and a
# broker certificate for localhost and 127.0.0.1, which the conformance
# suite and the quic example trust through root.pem.
set -eu
cd /pki

# The key is a test fixture read by the emqx user, hence world-readable.
umask 022

if [ ! -f ready ]; then
	find . -mindepth 1 ! -name .gitignore -delete
	cat >ext.cnf <<'EOF'
[req]
distinguished_name = dn
prompt = no
[dn]
[root]
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
[broker]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost, IP:127.0.0.1
authorityKeyIdentifier = keyid
EOF
	openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out root.key
	openssl req -new -x509 -key root.key -subj "/CN=mqttv5 QUIC conformance root" \
		-config ext.cnf -extensions root -days 3650 -out root.pem
	openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out broker.key
	openssl req -new -key broker.key -subj "/CN=localhost" -config ext.cnf -out broker.csr
	openssl x509 -req -in broker.csr -CA root.pem -CAkey root.key -sha256 -set_serial 0x1001 \
		-days 3650 -extfile ext.cnf -extensions broker -out broker.pem
	rm broker.csr
	chmod 644 root.key broker.key
	touch ready
	# Root wrote the files; give them to the owner of the bind-mounted
	# directory, the checkout's user.
	chown -R "$(stat -c %u:%g .)" .
fi
