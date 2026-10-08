// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package readme

import (
	"log"

	"github.com/ashtonian/mqttv5"
)

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
				Broker: "mqtts://emea.example.com:8883",
				Name:   "emea",
				Opts:   []mqttv5.Option{mqttv5.WithCredentials("emea-svc", []byte(token1))},
			},
			{
				Broker: "mqtts://apac.example.com:8883",
				Name:   "apac",
				Opts:   []mqttv5.Option{mqttv5.WithCredentials("apac-svc", []byte(token2))},
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
