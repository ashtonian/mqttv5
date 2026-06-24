// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
)

const (
	scramUser = "reauthuser"
	scramPass = "reauthpass"

	// API key bootstrapped into emqx-scram via ./emqx/api_key.bootstrap.
	scramAPIKey    = "conformancekey"
	scramAPISecret = "conformanceSecret0123456789"
)

// scramBrokerURL is the dedicated SCRAM-authenticated EMQX listener
// (emqx-scram in docker-compose), overridable for non-default setups.
func scramBrokerURL() string {
	if v := os.Getenv("MQTT_BROKER_SCRAM"); v != "" {
		return v
	}
	return "mqtt://127.0.0.1:1885"
}

// scramAPIBase is the emqx-scram REST API used to provision the SCRAM
// authenticator and user.
func scramAPIBase() string {
	if v := os.Getenv("EMQX_SCRAM_API"); v != "" {
		return v
	}
	return "http://127.0.0.1:18084/api/v5"
}

// provisionSCRAM ensures EMQX has the SCRAM authenticator and the test
// user. It is idempotent — an already-created authenticator/user (HTTP 409
// ALREADY_EXISTS) counts as success — and skips the test if the EMQX REST
// API is unreachable (start docker-compose first).
func provisionSCRAM(t *testing.T) {
	t.Helper()
	base := scramAPIBase()
	emqxProvision(t, base+"/authentication",
		`{"mechanism":"scram","backend":"built_in_database","algorithm":"sha256","iteration_count":4096}`)
	emqxProvision(t, base+"/authentication/scram:built_in_database/users",
		`{"user_id":"`+scramUser+`","password":"`+scramPass+`"}`)
}

func emqxProvision(t *testing.T, url, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build provisioning request: %v", err)
	}
	req.SetBasicAuth(scramAPIKey, scramAPISecret)
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Skipf("conformance: EMQX SCRAM API %s unreachable (%v) — start docker-compose first", url, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		// created
	case resp.StatusCode == http.StatusConflict && strings.Contains(string(respBody), "ALREADY_EXISTS"):
		// already provisioned by an earlier run — fine
	default:
		t.Fatalf("provisioning POST %s -> HTTP %d: %s", url, resp.StatusCode, respBody)
	}
}

// TestReauthenticate_SCRAM_EMQX exercises client-initiated MQTT 5
// re-authentication (§4.12) end-to-end against a real broker. It connects
// to EMQX using SCRAM-SHA-256 enhanced authentication, then calls
// Reauthenticate twice — each presenting a fresh client nonce — and
// asserts the broker concludes every exchange with AUTH 0x00 Success
// without dropping the live connection. The OnReauthenticated hook is
// expected to fire once per successful re-auth.
func TestReauthenticate_SCRAM_EMQX(t *testing.T) {
	url := scramBrokerURL()
	requireBroker(t, url)
	provisionSCRAM(t)

	var reauthed atomic.Int32
	auth := newSCRAM(scramUser, scramPass)
	cli, err := mqttv5.New(
		mqttv5.WithBroker(url),
		mqttv5.WithClientID("conf-scram-reauth-"+randSuffix()),
		mqttv5.WithAuthenticator(auth),
		mqttv5.WithConnectTimeout(5*time.Second),
		mqttv5.WithOnReauthenticated(func() { reauthed.Add(1) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect (SCRAM enhanced auth): %v", err)
	}
	t.Cleanup(func() {
		sd, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = cli.Disconnect(sd)
	})

	for i := 1; i <= 2; i++ {
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := cli.Reauthenticate(rctx)
		rcancel()
		if err != nil {
			t.Fatalf("Reauthenticate #%d: %v", i, err)
		}
		if !cli.Connected() {
			t.Fatalf("Connected() = false after re-auth #%d (the connection must survive)", i)
		}
	}

	if got := reauthed.Load(); got != 2 {
		t.Errorf("OnReauthenticated fired %d times, want 2", got)
	}
	// SCRAM mutual auth ran for the initial connect + both re-auths.
	if got := auth.verifies.Load(); got != 3 {
		t.Errorf("SCRAM server-signature verifications = %d, want 3 (connect + 2 re-auths)", got)
	}
}
