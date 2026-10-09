// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package readme

import "github.com/ashtonian/mqttv5"

func publish() {
	err := cli.Publish(ctx, mqttv5.PublishOptions{
		Topic:   "sensors/a1/temp",
		QoS:     1,
		Payload: []byte(`{"temp":22.5}`),
	})
	_ = err
}
