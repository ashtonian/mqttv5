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

func publisherPool() {
	cli, _ := mqttv5.New(
		mqttv5.WithBroker("mqtts://mqtt.example.com:8883"),
		mqttv5.WithClientID("telemetry-gw"),
		mqttv5.WithPublisherPool(4), // members telemetry-gw-pub-1 … -4
		mqttv5.WithPublisherPoolRouting(mqttv5.PoolRoutingHashByTopic),
	)
	_ = cli
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

func durableConsumer() {
	cli, _ := mqttv5.New(
		mqttv5.WithBroker(url),
		mqttv5.WithClientID("billing-worker-1"),
		mqttv5.WithCleanStart(false),       // resume the session after a restart
		mqttv5.WithSessionExpiry(24*60*60), // the broker keeps it a day
	)
	msgs, _, _ := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "invoices/#", QoS: 1}})
	if err := cli.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	for m := range msgs { // first what arrived while the worker was down
		handle(m)
		_ = m.Ack()
	}
}

func price([]byte) []byte { return nil }

func responder() {
	// Responder: reply on each request's Response Topic, with its
	// Correlation Data. A shared subscription spreads requests over instances.
	reqs, _, _ := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "$share/pricing/requests/price", QoS: 1}})
	for m := range reqs {
		if rt := m.Properties.ResponseTopic(); rt != "" {
			_ = cli.Publish(ctx, mqttv5.PublishOptions{
				Topic:           rt,
				CorrelationData: m.Properties.CorrelationData(),
				QoS:             1,
				Payload:         price(m.Payload),
			})
		}
		_ = m.Ack()
	}
}

func requester() {
	// Requester: one reply subscription serves every request in flight.
	replyTo := "replies/" + cli.ClientID()
	replies, _, _ := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: replyTo, QoS: 1}})
	_ = cli.Publish(ctx, mqttv5.PublishOptions{
		Topic:           "requests/price",
		QoS:             1,
		Payload:         []byte("A-100"),
		ResponseTopic:   replyTo,
		CorrelationData: []byte("req-1"), // matches the reply to this call
	})
	_ = replies
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
