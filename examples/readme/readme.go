// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package readme holds every Go snippet of the repository README in a
// form that compiles, so the README cannot drift from the API:
// TestREADMESnippets fails when a README snippet is not found, line for
// line, in one of these files. The functions are never called.
package readme

import (
	"context"
	"fmt"
	"log"

	"github.com/ashtonian/mqttv5"
)

var (
	ctx    = context.Background()
	cli    *mqttv5.Client
	g      *mqttv5.ClientGroup
	p      []byte
	data   []byte
	url    string
	broker string
	token1 string
	token2 string
)

func handle(*mqttv5.Message) {}

func process(*mqttv5.Message) {}

func publish() {
	err := cli.Publish(ctx, mqttv5.PublishOptions{
		Topic:   "sensors/a1/temp",
		QoS:     1,
		Payload: []byte(`{"temp":22.5}`),
	})
	_ = err
}

func subscribeChannel() {
	msgs, token, err := cli.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}},
		mqttv5.SubBuffer(256),
	)
	for m := range msgs {
		handle(m)
		_ = m.Ack() // PUBACK released in §4.6 arrival order
	}
	_ = cli.Unsubscribe(ctx, token) // closes msgs
	_ = err
}

func subscribeQueue() {
	q, _, _ := cli.SubscribeQueue(ctx,
		[]mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}},
		mqttv5.SubMaxQueueSize(10_000),
		mqttv5.SubDropPolicy(mqttv5.DropOldest), // keeps freshest 10k
	)
	for {
		m, ok := q.Dequeue(ctx)
		if !ok {
			break
		}
		handle(m)
		_ = m.Ack()
	}
}

func subscribeCallback() {
	cli.SubscribeCallback(ctx,
		[]mqttv5.TopicFilter{{Topic: "ctrl/+", QoS: 0}},
		func(m *mqttv5.Message) {
			// Runs on the read goroutine — MUST be non-blocking.
			process(m)
			// Ack auto-fires after return.
		},
	)
}

func redirects() {
	cli, _ := mqttv5.New(
		mqttv5.WithBroker("mqtts://broker-a.example.com:8883"),
		mqttv5.WithFollowServerRedirects(),
		mqttv5.WithOnServerRedirect(func(r mqttv5.ServerRedirect) {
			log.Printf("redirected to %s (permanent=%v)", r.Reference, r.Permanent())
		}),
	)
	_ = cli
}

func group() {
	g, _ := mqttv5.NewClientGroup(
		[]mqttv5.GroupMember{
			{
				Brokers: []string{"mqtts://emea.example.com:8883"},
				Name:    "emea",
				Opts:    []mqttv5.Option{mqttv5.WithCredentials("emea-svc", []byte(token1))},
			},
			{
				Brokers: []string{"mqtts://apac.example.com:8883"},
				Name:    "apac",
				Opts:    []mqttv5.Option{mqttv5.WithCredentials("apac-svc", []byte(token2))},
			},
		},
		mqttv5.WithGroupSharedOpts(
			mqttv5.WithClientID("fleet"),
			mqttv5.WithKeepAlive(30),
		),
		mqttv5.WithGroupPublishPolicy(mqttv5.GroupPublishBroadcast),
	)
	_ = g
}

func groupPublish() {
	results, err := g.Publish(ctx, mqttv5.PublishOptions{Topic: "events/x", QoS: 1, Payload: p})
	for _, r := range results {
		if r.Err != nil {
			log.Printf("%s: %v", r.Member, r.Err) // errors.Is(err, mqttv5.ErrNotAuthorized) works on the GroupError too
		}
	}
	_ = err
}

func groupSubscribe() {
	ch, tokens, err := g.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}})
	// tokens["emea"], tokens["apac"]
	defer g.UnsubscribeAll(ctx, tokens)
	_, _ = ch, err
}

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
