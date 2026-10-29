package readme

import (
	"log"
	"time"

	"github.com/ashtonian/mqttv5"
)

import (
	qfile "github.com/ashtonian/mqttv5/queue/file"
	sfile "github.com/ashtonian/mqttv5/store/file"
)

func queuePublisher() {
	st, _ := sfile.Open("/var/lib/myapp/session")
	cli, _ := mqttv5.New(mqttv5.WithBroker(url), mqttv5.WithClientID("dev-1"), mqttv5.WithStore(st))
	q, _ := qfile.Open("/var/lib/myapp/outbound")
	pub, _ := mqttv5.NewQueuePublisher(cli, q,
		mqttv5.WithQueueMaxSize(1_000_000),
		mqttv5.WithQueueTTL(24*time.Hour),
		mqttv5.WithDeadLetter(func(e mqttv5.QueueEntry, err error) {
			log.Printf("dropped %s: %v", e.Publish.Topic, err)
		}),
	)
	defer pub.Close(ctx)

	_ = pub.Publish(ctx, mqttv5.PublishOptions{Topic: "logs", Payload: data, QoS: 1})
}

func durableOutbox() {
	st, _ := sfile.Open("/var/lib/gateway/session")
	q, _ := qfile.Open("/var/lib/gateway/outbound")
	cli, _ := mqttv5.New(
		mqttv5.WithBrokers("mqtts://mqtt-1.example.com:8883", "mqtts://mqtt-2.example.com:8883"),
		mqttv5.WithClientID("gateway-12"),
		mqttv5.WithStore(st),
		mqttv5.WithRetryInitialConnect(),
	)
	pub, _ := mqttv5.NewQueuePublisher(cli, q, mqttv5.WithQueueIdempotencyKey())
	_ = pub
}
