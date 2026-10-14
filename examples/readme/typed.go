// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package readme

import (
	"fmt"

	"github.com/ashtonian/mqttv5"
)

import jsoncodec "github.com/ashtonian/mqttv5/codec/json"

type Reading struct {
	Device string
	Temp   float64
}

type Command struct {
	Op string
}

func typedPubSub() {
	cli, _ := mqttv5.New(
		mqttv5.WithBroker(url),
		mqttv5.WithEncoding(jsoncodec.Encoding{}), // one Encoding, any payload type
	)

	readings, _, _ := cli.SubscribeAs[Reading](ctx,
		[]mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}})
	commands, _, _ := cli.SubscribeAs[Command](ctx,
		[]mqttv5.TopicFilter{{Topic: "devices/+/cmd", QoS: 1}})

	// Declares Content Type application/json.
	_ = cli.PublishValue(ctx, mqttv5.PublishOptions{Topic: "sensors/a1", QoS: 1},
		Reading{Device: "a1", Temp: 22.5})

	for {
		select {
		case m := <-readings:
			fmt.Println(m.Topic, m.Value.Temp)
			_ = m.Ack()
		case m := <-commands:
			fmt.Println(m.Topic, m.Value.Op)
			_ = m.Ack()
		}
	}
}
