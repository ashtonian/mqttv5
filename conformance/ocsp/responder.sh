#!/bin/sh
# Builds the OCSP conformance PKI into /pki once, then serves OCSP for it
# with OpenSSL's responder: the root's on port 2561, signed with the
# root's own key, and the CA's on port 2560, signed by a delegated
# responder certificate. The emqx-ocsp listeners serve the broker
# certificates; ocsp_test.go reads root.pem.
#
# Serials (hex): root 01, CA 10, delegated responder 20, brokers 1001
# (good), 1002 (revoked) and 1003 (good, must-staple).
set -eu
cd /pki

# The keys are test fixtures read by the emqx user, hence world-readable.
umask 022

issue() { # name, serial, extensions section, subject, issuer name, digest
	openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1.key"
	openssl req -new -key "$1.key" -subj "$4" -config ext.cnf -out "$1.csr"
	if [ "$5" = self ]; then
		signer="-key $1.key"
	else
		signer="-CA $5.pem -CAkey $5.key"
	fi
	# shellcheck disable=SC2086 # signer is two options
	openssl x509 -req -in "$1.csr" $signer -"${6:-sha256}" -set_serial "0x$2" -days 3650 \
		-extfile ext.cnf -extensions "$3" -out "$1.pem"
	rm "$1.csr"
	chmod 644 "$1.key"
}

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
[ca]
basicConstraints = critical, CA:true, pathlen:0
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
authorityInfoAccess = OCSP;URI:http://127.0.0.1:2561
[responder]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature
extendedKeyUsage = OCSPSigning
noCheck = ignored
[broker]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost, IP:127.0.0.1
authorityInfoAccess = OCSP;URI:http://127.0.0.1:2560
[must_staple]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost, IP:127.0.0.1
authorityInfoAccess = OCSP;URI:http://127.0.0.1:2560
tlsfeature = status_request
EOF
	issue root 01 root "/CN=mqttv5 OCSP conformance root" self
	issue ca 10 ca "/CN=mqttv5 OCSP conformance CA" root
	issue responder 20 responder "/CN=mqttv5 OCSP conformance responder" ca
	issue good 1001 broker "/CN=good broker" ca
	# EMQX 6.3.1 keys its OCSP cache by a certificate's signature
	# algorithm (emqx_ocsp_cache:cache_key/1 reads tbsCertificate's
	# AlgorithmIdentifier, not the signature), so stapling listeners whose
	# certificates share an algorithm staple one response between them. A
	# different digest keeps the two stapling listeners apart.
	issue revoked 1002 broker "/CN=revoked broker" ca sha384
	issue must-staple 1003 must_staple "/CN=must-staple broker" ca
	for b in good revoked must-staple; do
		cat "$b.pem" ca.pem >"$b-chain.pem"
	done

	# OpenSSL CA databases: status, expiry, revocation, serial, file,
	# subject. Only status, revocation and serial matter to the responder.
	tab=$(printf '\t')
	cat >root-index.txt <<EOF
V${tab}360101000000Z${tab}${tab}10${tab}unknown${tab}/CN=mqttv5 OCSP conformance CA
EOF
	cat >ca-index.txt <<EOF
V${tab}360101000000Z${tab}${tab}1001${tab}unknown${tab}/CN=good broker
R${tab}360101000000Z${tab}260101000000Z,keyCompromise${tab}1002${tab}unknown${tab}/CN=revoked broker
V${tab}360101000000Z${tab}${tab}1003${tab}unknown${tab}/CN=must-staple broker
EOF
	touch ready
	# Root wrote the files; give them to the owner of the bind-mounted
	# directory, the checkout's user.
	chown -R "$(stat -c %u:%g .)" .
fi

openssl ocsp -index root-index.txt -CA root.pem -rsigner root.pem -rkey root.key \
	-port 2561 -ndays 1 -ignore_err &
exec openssl ocsp -index ca-index.txt -CA ca.pem -rsigner responder.pem -rkey responder.key \
	-port 2560 -ndays 1 -ignore_err
