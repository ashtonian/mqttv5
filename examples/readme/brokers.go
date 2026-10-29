package readme

import (
	"context"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ashtonian/mqttv5"
)

var (
	members []mqttv5.GroupMember
	shards  []mqttv5.GroupMember
	q       mqttv5.PublisherQueue
)

func commandService() {
	svc, _ := mqttv5.NewClientGroup(
		[]mqttv5.GroupMember{
			{Name: "a", Brokers: []string{"mqtts://a-1.example.com:8883", "mqtts://a-2.example.com:8883"}},
			{Name: "b", Brokers: []string{"mqtts://b-1.example.com:8883", "mqtts://b-2.example.com:8883"}},
		},
		mqttv5.WithGroupSharedOpts(
			mqttv5.WithClientID("command-service"),
			mqttv5.WithCleanStart(false),
			mqttv5.WithSessionExpiry(60*60),
			mqttv5.WithRetryInitialConnect(), // a cluster down at start is tried again
		),
		mqttv5.WithGroupSuccess(mqttv5.GroupSuccessAny), // one cluster down fails no command
	)
	// Replies from devices on either cluster, in one channel.
	replies, _, _ := svc.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "devices/+/reply", QoS: 1}})
	if err := svc.Connect(ctx); err != nil {
		log.Fatal(err)
	}

	// One command, to both clusters.
	expiry := uint32(60)
	_, err := svc.Publish(ctx, mqttv5.PublishOptions{
		Topic:                 "devices/42/cmd",
		QoS:                   1,
		Payload:               []byte(`{"desired":{"firmware":"1.4.2"}}`),
		CorrelationData:       []byte("c7"),
		MessageExpiryInterval: &expiry,
	})
	_, _ = replies, err
}

func failover() {
	cli, _ := mqttv5.New(
		mqttv5.WithBrokers(
			"mqtts://mqtt-1.example.com:8883",
			"mqtts://mqtt-2.example.com:8883",
			"mqtts://mqtt-3.example.com:8883",
		),
		mqttv5.WithClientID("ingest-7"),
		mqttv5.WithOnConnectionUp(func(info mqttv5.ConnackInfo) {
			log.Printf("connected to %s, session present: %v", info.Broker, info.SessionPresent)
		}),
		mqttv5.WithOnReconnectAttempt(func(attempt int, broker string) {
			log.Printf("reconnect attempt %d: %s", attempt, broker)
		}),
	)
	_ = cli
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

func bridge() error {
	src, _ := mqttv5.New(
		mqttv5.WithBroker("mqtts://plant.example.com:8883"),
		mqttv5.WithClientID("bridge-plant-3"),
		mqttv5.WithCleanStart(false),       // resume the source's session after a restart
		mqttv5.WithSessionExpiry(24*60*60), // the source holds messages while the bridge is down
	)
	dst, _ := mqttv5.New(
		mqttv5.WithBroker("mqtts://cloud.example.com:8883"),
		mqttv5.WithClientID("bridge-plant-3"),
	)
	// Subscribe before Connect, so the messages held for the bridge reach it.
	msgs, _, _ := src.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "plant/#", QoS: 1}})
	// Connect dst, then src, then:
	for m := range msgs {
		if err := forward(ctx, dst, m); err != nil {
			return err // m stays unacked: the source delivers it again
		}
		_ = m.Ack()
	}
	return nil
}

// forward publishes m on dst under "site-3/", waiting out dst's
// outages. It returns nil once dst has the message or refused it for
// good, and an error when ctx ends or dst is closed.
func forward(ctx context.Context, dst *mqttv5.Client, m *mqttv5.Message) error {
	opts := m.PublishOptions() // payload and properties as they arrived
	opts.Topic = "site-3/" + m.Topic
	for {
		err := dst.Publish(ctx, opts)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, mqttv5.ErrNotConnected):
			if err = dst.AwaitConnection(ctx); err != nil {
				return err
			}
		case ctx.Err() != nil, errors.Is(err, mqttv5.ErrClosed):
			return err
		default:
			log.Printf("destination refused %s: %v", opts.Topic, err)
			return nil // drop it rather than hold up every message behind it
		}
	}
}

func durableBridge(dst *mqttv5.Client, msgs <-chan *mqttv5.Message) error {
	out, _ := mqttv5.NewQueuePublisher(dst, q) // q from qfile.Open
	for m := range msgs {
		opts := m.PublishOptions()
		opts.Topic = "site-3/" + m.Topic
		opts.QoS = max(opts.QoS, 1) // the queue takes QoS 1 and 2
		if err := out.Publish(ctx, opts); err != nil {
			return err // not stored: the source delivers it again
		}
		_ = m.Ack()
	}
	return nil
}

func regionalHA() {
	g, _ := mqttv5.NewClientGroup(
		[]mqttv5.GroupMember{
			{Name: "eu", Brokers: []string{"mqtts://eu-1.example.com:8883", "mqtts://eu-2.example.com:8883"}},
			{Name: "us", Brokers: []string{"mqtts://us-1.example.com:8883", "mqtts://us-2.example.com:8883"}},
		},
		mqttv5.WithGroupSharedOpts(mqttv5.WithClientID("pricing")),
		mqttv5.WithGroupSuccess(mqttv5.GroupSuccessAny),
	)
	_ = g
}

func quorum() {
	g, _ := mqttv5.NewClientGroup(members, // three independent brokers
		mqttv5.WithGroupSuccess(mqttv5.GroupSuccessQuorum(2)),
	)
	results, err := g.Publish(ctx, mqttv5.PublishOptions{Topic: "ledger/tx", QoS: 1, Payload: p})
	// err is nil once two of the three acknowledged; results has each one's outcome
	_, _ = results, err
}

func sharding() {
	// Publishers: each topic goes to the same broker while it is connected.
	pubs, _ := mqttv5.NewClientGroup(shards,
		mqttv5.WithGroupSharedOpts(mqttv5.WithClientID("orders-pub")),
		mqttv5.WithGroupPublishPolicy(mqttv5.GroupPublishHashByTopic),
	)

	// Consumers: one merged subscription across every shard; each Ack goes
	// back to the broker that delivered the message.
	subs, _ := mqttv5.NewClientGroup(shards, mqttv5.WithGroupSharedOpts(mqttv5.WithClientID("orders-sub")))
	msgs, tokens, _ := subs.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "orders/#", QoS: 1}})
	_, _, _ = pubs, msgs, tokens
}

// watchBrokers keeps cli's broker list at the _mqtt._tcp SRV records
// of example.com, checked every minute.
func watchBrokers(ctx context.Context, cli *mqttv5.Client) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		_, srvs, err := net.DefaultResolver.LookupSRV(ctx, "mqtt", "tcp", "example.com")
		if err == nil {
			urls := make([]string, 0, len(srvs))
			for _, s := range srvs {
				urls = append(urls, "mqtt://"+net.JoinHostPort(strings.TrimSuffix(s.Target, "."), strconv.Itoa(int(s.Port))))
			}
			err = cli.SetBrokers(urls...)
		}
		if err != nil {
			log.Printf("broker list unchanged: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func sharedSubscription() {
	// In every instance:
	jobs, _, _ := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "$share/workers/jobs/#", QoS: 1}})
	_ = jobs
}

func primaryAndStandby() {
	cli, _ := mqttv5.New(
		mqttv5.WithBrokers(
			"mqtts://primary.example.com:8883",
			"mqtts://standby.example.com:8883",
		),
		mqttv5.WithFailoverOrder(mqttv5.FailoverPriority),
		mqttv5.WithClientID("ingest-7"),
	)
	_ = cli
}
