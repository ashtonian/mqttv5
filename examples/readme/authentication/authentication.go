// Package authentication holds the Go snippets of
// docs/authentication.md that name net/url, whose name the readme
// package uses for a variable.
package authentication

import (
	"context"
	"net/url"

	"github.com/ashtonian/mqttv5"
)

type tokenSource struct{}

func (tokenSource) FetchToken(context.Context, string) (string, error) { return "", nil }

var oauth tokenSource

func credentialRotation() {
	_, _ = mqttv5.New(
		mqttv5.WithConnectPacketBuilder(func(ctx context.Context, brokerURL *url.URL, opts *mqttv5.ConnectOptions) error {
			tok, err := oauth.FetchToken(ctx, brokerURL.Hostname()) // a token for this broker
			if err != nil {
				return err // fails this attempt; supervisor retries after backoff
			}
			opts.Username = "service-account"
			opts.Password = []byte(tok)
			return nil
		}),
	)
}
