// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package readme

import "github.com/ashtonian/mqttv5"

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
