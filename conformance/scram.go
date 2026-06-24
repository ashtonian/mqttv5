// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// scramSHA256 is a minimal client-side SCRAM-SHA-256 (RFC 5802)
// Authenticator used to exercise MQTT v5 enhanced authentication and
// client-initiated re-authentication (§4.12) against a real broker (EMQX).
//
// It implements just enough of SCRAM to drive the n=.../r=.../p=...
// exchange the broker validates. mqttv5 concludes the exchange on CONNACK
// (initial connect) or AUTH 0x00 (re-auth) without re-invoking Continue,
// so the optional server-signature verification (mutual auth) step is not
// performed — the broker's acceptance is the success signal.
//
// A fresh client nonce is generated per Begin, so every (re-)auth is a
// distinct exchange. Begin runs on the caller/connect goroutine and
// Continue on the read loop; the mutex guards the nonce handed between
// them.
type scramSHA256 struct {
	username string
	password string

	mu          sync.Mutex
	clientNonce string
	firstBare   string
	serverSig   []byte // expected server signature, computed in Continue

	// verifies counts successful VerifyServerFinal calls, so tests can
	// assert mutual authentication actually ran (connect + each re-auth).
	verifies atomic.Int32
}

func newSCRAM(username, password string) *scramSHA256 {
	return &scramSHA256{username: username, password: password}
}

// Method returns the SCRAM-SHA-256 AuthenticationMethod name.
func (*scramSHA256) Method() string { return "SCRAM-SHA-256" }

// Begin emits the SCRAM client-first message: "n,,n=<user>,r=<nonce>".
func (s *scramSHA256) Begin(_ context.Context) ([]byte, error) {
	nonce, err := randomNonce()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.clientNonce = nonce
	s.firstBare = "n=" + saslName(s.username) + ",r=" + nonce
	first := s.firstBare
	s.mu.Unlock()
	// GS2 header "n,," (no channel binding, no authzid) + first-bare.
	return []byte("n,," + first), nil
}

// Continue consumes the server-first message and returns the client-final
// message carrying the computed proof.
func (s *scramSHA256) Continue(serverFirst []byte) ([]byte, bool, error) {
	s.mu.Lock()
	clientNonce, firstBare := s.clientNonce, s.firstBare
	s.mu.Unlock()

	r, salt, iter, err := parseServerFirst(string(serverFirst))
	if err != nil {
		return nil, false, err
	}
	if !strings.HasPrefix(r, clientNonce) {
		return nil, false, fmt.Errorf("scram: server nonce %q does not extend client nonce", r)
	}

	saltedPassword, err := pbkdf2.Key(sha256.New, s.password, salt, iter, sha256.Size)
	if err != nil {
		return nil, false, fmt.Errorf("scram: pbkdf2: %w", err)
	}
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)

	finalNoProof := "c=biws,r=" + r // biws = base64("n,,")
	authMessage := firstBare + "," + string(serverFirst) + "," + finalNoProof
	clientSig := hmacSHA256(storedKey[:], []byte(authMessage))

	// Server signature, retained for mutual-auth verification of the
	// server-final message (see VerifyServerFinal).
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))
	s.mu.Lock()
	s.serverSig = hmacSHA256(serverKey, []byte(authMessage))
	s.mu.Unlock()

	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	final := finalNoProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	return []byte(final), false, nil
}

// VerifyServerFinal completes SCRAM mutual authentication: it checks the
// server-final "v=<base64-serversig>" against the server signature
// computed in Continue (RFC 5802 §5.1). It implements
// mqttv5.ServerFinalVerifier, so mqttv5 calls it with the broker's
// concluding AuthenticationData (CONNACK on connect, AUTH 0x00 on
// re-auth). A mismatch means the server did not prove knowledge of the
// credential.
func (s *scramSHA256) VerifyServerFinal(serverFinal []byte) error {
	s.mu.Lock()
	want := s.serverSig
	s.mu.Unlock()
	if len(want) == 0 {
		return fmt.Errorf("scram: no server signature computed (VerifyServerFinal before Continue)")
	}
	got, err := parseServerFinal(string(serverFinal))
	if err != nil {
		return err
	}
	if !hmac.Equal(got, want) {
		return fmt.Errorf("scram: server signature mismatch — server not authenticated")
	}
	s.verifies.Add(1)
	return nil
}

// parseServerFinal extracts the server signature from a SCRAM server-final
// message "v=<base64>", or surfaces a server error field "e=<reason>".
func parseServerFinal(msg string) ([]byte, error) {
	for _, field := range strings.Split(msg, ",") {
		switch {
		case strings.HasPrefix(field, "v="):
			sig, err := base64.StdEncoding.DecodeString(field[2:])
			if err != nil {
				return nil, fmt.Errorf("scram: bad server signature: %w", err)
			}
			return sig, nil
		case strings.HasPrefix(field, "e="):
			return nil, fmt.Errorf("scram: server reported error: %s", field[2:])
		}
	}
	return nil, fmt.Errorf("scram: server-final missing v= field (%q)", msg)
}

func hmacSHA256(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(msg)
	return h.Sum(nil)
}

func randomNonce() (string, error) {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// parseServerFirst extracts r (nonce), s (salt), i (iterations) from a
// SCRAM server-first message: "r=<nonce>,s=<b64-salt>,i=<iterations>".
func parseServerFirst(msg string) (nonce string, salt []byte, iter int, err error) {
	for _, field := range strings.Split(msg, ",") {
		if len(field) < 2 || field[1] != '=' {
			continue
		}
		val := field[2:]
		switch field[0] {
		case 'r':
			nonce = val
		case 's':
			salt, err = base64.StdEncoding.DecodeString(val)
			if err != nil {
				return "", nil, 0, fmt.Errorf("scram: bad salt: %w", err)
			}
		case 'i':
			iter, err = strconv.Atoi(val)
			if err != nil {
				return "", nil, 0, fmt.Errorf("scram: bad iteration count: %w", err)
			}
		}
	}
	if nonce == "" || len(salt) == 0 || iter == 0 {
		return "", nil, 0, fmt.Errorf("scram: incomplete server-first %q", msg)
	}
	return nonce, salt, iter, nil
}

// saslName applies the comma/equals escaping SCRAM requires for the
// username (RFC 5802 §5.1). Our test usernames contain neither, but keep
// it correct.
func saslName(s string) string {
	s = strings.ReplaceAll(s, "=", "=3D")
	s = strings.ReplaceAll(s, ",", "=2C")
	return s
}
