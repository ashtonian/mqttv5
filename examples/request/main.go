// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Request example: MQTT 5 request/response. A responder serves
// requests/price on a shared subscription, so several can share the
// load; a requester sends each request with a Response Topic of its own
// and Correlation Data, and matches the replies to the calls waiting
// for them.
//
//	go -C examples run ./request
package main

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	interrupted := ctx.Err() != nil
	stop()
	// An interrupt cancels whatever run was doing: that is a clean stop.
	if interrupted && errors.Is(err, context.Canceled) {
		err = nil
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "request:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	server, err := mqttv5.New(mqttv5.WithBroker(broker), mqttv5.WithClientID("mqttv5-request-server"), mqttv5.WithLogger(logger))
	if err != nil {
		return err
	}
	if err = serve(ctx, server); err != nil {
		return err
	}
	defer func() { _ = server.Disconnect(context.WithoutCancel(ctx)) }()

	client, err := mqttv5.New(mqttv5.WithBroker(broker), mqttv5.WithClientID("mqttv5-request-client"), mqttv5.WithLogger(logger))
	if err != nil {
		return err
	}
	req, err := newRequester(ctx, client)
	if err != nil {
		return err
	}
	if err := client.Connect(ctx); err != nil {
		return err
	}
	defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()

	// Requests in flight at once, each matched to its reply.
	skus := []string{"A-100", "B-200", "C-300"}
	errs := make([]error, len(skus))
	var wg sync.WaitGroup
	for i, sku := range skus {
		wg.Go(func() {
			rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			reply, err := req.call(rctx, "requests/price", []byte(sku))
			if err != nil {
				errs[i] = fmt.Errorf("%s: %w", sku, err)
				return
			}
			fmt.Printf("%s: %s\n", sku, reply)
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// prices is the responder's data.
var prices = map[string]string{"A-100": "12.50", "B-200": "7.25", "C-300": "99.00"}

// serve answers every request with a price, published to the request's
// Response Topic with its Correlation Data.
func serve(ctx context.Context, cli *mqttv5.Client) error {
	reqs, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "$share/pricing/requests/price", QoS: 1}})
	if err != nil {
		return err
	}
	if err := cli.Connect(ctx); err != nil {
		return err
	}
	go func() {
		for m := range reqs {
			if rt := m.Properties.ResponseTopic(); rt != "" {
				err := cli.Publish(ctx, mqttv5.PublishOptions{
					Topic:           rt,
					QoS:             1,
					CorrelationData: m.Properties.CorrelationData(),
					Payload:         []byte(cmp.Or(prices[string(m.Payload)], "unknown")),
				})
				if err != nil && !errors.Is(err, mqttv5.ErrClosed) {
					fmt.Printf("reply to %s: %v\n", m.Payload, err)
				}
			}
			_ = m.Ack()
		}
	}()
	return nil
}

// requester sends requests and hands each reply to the call waiting for
// it, by Correlation Data. One reply subscription serves every call.
type requester struct {
	cli     *mqttv5.Client
	replyTo string
	next    atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]chan []byte
}

func newRequester(ctx context.Context, cli *mqttv5.Client) (*requester, error) {
	r := &requester{
		cli:     cli,
		replyTo: "replies/" + strings.ReplaceAll(cli.ClientID(), "/", "_"),
		pending: make(map[uint64]chan []byte),
	}
	replies, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: r.replyTo, QoS: 1}})
	if err != nil {
		return nil, err
	}
	go func() {
		for m := range replies {
			if cd := m.Properties.CorrelationData(); len(cd) == 8 {
				r.mu.Lock()
				ch := r.pending[binary.BigEndian.Uint64(cd)]
				r.mu.Unlock()
				select {
				case ch <- m.Payload:
				default: // no call waits for it, or a duplicate
				}
			}
			_ = m.Ack()
		}
	}()
	return r, nil
}

// call publishes a request to topic and waits for its reply until ctx
// ends.
func (r *requester) call(ctx context.Context, topic string, payload []byte) ([]byte, error) {
	id := r.next.Add(1)
	ch := make(chan []byte, 1)
	r.mu.Lock()
	r.pending[id] = ch
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.pending, id)
		r.mu.Unlock()
	}()

	err := r.cli.Publish(ctx, mqttv5.PublishOptions{
		Topic:           topic,
		QoS:             1,
		Payload:         payload,
		ResponseTopic:   r.replyTo,
		CorrelationData: binary.BigEndian.AppendUint64(nil, id),
	})
	if err != nil {
		return nil, err
	}
	select {
	case reply := <-ch:
		return reply, nil
	case <-ctx.Done():
		return nil, errors.Join(errors.New("no reply"), ctx.Err())
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
