// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

// TestDialDispatch covers the URL-scheme switch in Dial. We don't need
// a server up for the negative cases — they fail in URL parsing or
// connect-refused before any bytes flow.
func TestDialDispatchUnknownScheme(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := Dial(ctx, "stomp://example.com:1883", DialOpts{})
	if !errors.Is(err, ErrUnknownScheme) {
		t.Fatalf("got %v, want ErrUnknownScheme", err)
	}
}

func TestDialDispatchMissingHost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := Dial(ctx, "mqtt://", DialOpts{})
	if !errors.Is(err, ErrMissingHost) {
		t.Fatalf("got %v, want ErrMissingHost", err)
	}
}

func TestDialTCPSuccess(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = c.Write([]byte("hi"))
		_ = c.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, "mqtt://"+ln.Addr().String(), DialOpts{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	buf := make([]byte, 2)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "hi" {
		t.Fatalf("read = %q, want %q", buf, "hi")
	}
}

func TestDialTCPCtxCancel(t *testing.T) {
	// Dialing an unroutable address must respect ctx cancellation.
	// 192.0.2.0/24 is RFC 5737 TEST-NET-1 — no host should answer.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Dial(ctx, "mqtt://192.0.2.1:1883", DialOpts{})
	if err == nil {
		t.Fatal("expected dial to fail")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("dial ignored ctx deadline (took %v)", time.Since(start))
	}
}

// TestDialTLS sets up a localhost TLS listener with a self-signed
// cert, then dials through Dial(... mqtts://). The client trusts the
// cert via InsecureSkipVerify (this is a unit test, not a TLS
// pen-test).
func TestDialTLS(t *testing.T) {
	cert := selfSignedCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		// Force the TLS handshake to complete before we write.
		if tc, ok := c.(*tls.Conn); ok {
			_ = tc.Handshake()
		}
		_, _ = c.Write([]byte("ok"))
		_ = c.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, "mqtts://"+ln.Addr().String(), DialOpts{
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
	})
	if err != nil {
		t.Fatalf("Dial tls: %v", err)
	}
	defer c.Close()

	buf := make([]byte, 2)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "ok" {
		t.Fatalf("read = %q, want %q", buf, "ok")
	}
}

// selfSignedCert generates a fresh self-signed ECDSA cert good for
// localhost over the next hour. Used only in tests.
func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        tmpl,
	}
}
