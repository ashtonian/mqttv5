// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package readme

import (
	"context"
	"fmt"

	"github.com/ashtonian/mqttv5"
)

func stats() {
	cli, _ := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithStats(),
	)
	// ...
	s := cli.Stats()
	fmt.Printf("sent=%d acked=%d inflight=%d connects=%d failures=%d\n",
		s.PublishesSent, s.PublishesAcked, s.PublishesInflight,
		s.Connects, s.ConnectFailures)
}

func gracefulDisconnect() {
	expiry := uint32(0)
	_ = cli.DisconnectWith(ctx, mqttv5.DisconnectOptions{
		ReasonCode:            mqttv5.ReasonAdministrativeAction,
		ReasonString:          "planned shutdown",
		SessionExpiryInterval: &expiry, // override to drop the session immediately
	})
}

type tokenSource struct{}

func (tokenSource) FetchToken(context.Context) (string, error) { return "", nil }

var oauth tokenSource

func credentialRotation() {
	_, _ = mqttv5.New(
		mqttv5.WithConnectPacketBuilder(func(ctx context.Context, opts *mqttv5.ConnectOptions) error {
			tok, err := oauth.FetchToken(ctx)
			if err != nil {
				return err // fails this attempt; supervisor retries after backoff
			}
			opts.Username = "service-account"
			opts.Password = []byte(tok)
			return nil
		}),
	)
}

func reauthenticate() {
	// e.g. 30s before the JWT `exp`:
	if err := cli.Reauthenticate(ctx); err != nil {
		// ErrReauthRejected → broker refused the new credential;
		// the supervisor is already reconnecting with a fresh CONNECT.
	}
}
