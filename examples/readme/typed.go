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

func typedPubSub() {
	typed := mqttv5.NewTyped[Reading](cli, jsoncodec.Codec[Reading]{})

	_ = typed.Publish(ctx, mqttv5.PublishOptions{Topic: "sensors/a1", QoS: 1},
		Reading{Device: "a1", Temp: 22.5})

	ch, _, _ := typed.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}})
	for m := range ch {
		fmt.Println(m.Topic, m.Value.Temp)
		_ = m.Ack()
	}
}
