package readme

import (
	"fmt"
	"log"

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

	// Subscribed before Connect, so nothing the broker sends is missed.
	readings, _, _ := cli.SubscribeAs[Reading](ctx,
		[]mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}})
	commands, _, _ := cli.SubscribeAs[Command](ctx,
		[]mqttv5.TopicFilter{{Topic: "devices/+/cmd", QoS: 1}})
	if err := cli.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer cli.Disconnect(ctx)

	// Declares Content Type application/json.
	_ = cli.PublishValue(ctx, mqttv5.PublishOptions{Topic: "sensors/a1", QoS: 1},
		Reading{Device: "a1", Temp: 22.5})

	for {
		select {
		case m, ok := <-readings:
			if !ok {
				return
			}
			fmt.Println(m.Topic, m.Value.Temp)
			_ = m.Ack()
		case m, ok := <-commands:
			if !ok {
				return
			}
			fmt.Println(m.Topic, m.Value.Op)
			_ = m.Ack()
		}
	}
}
