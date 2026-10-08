// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5_test

import (
	"testing"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/queuetest"
)

func TestMemoryPublisherQueueConformance(t *testing.T) {
	queuetest.Run(t, queuetest.Factory{
		Open: func(*testing.T) mqttv5.PublisherQueue { return mqttv5.NewMemoryPublisherQueue() },
	})
}
