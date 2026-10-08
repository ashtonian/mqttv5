// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session_test

import (
	"testing"

	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/session/storetest"
)

func TestMemoryStoreConformance(t *testing.T) {
	storetest.Run(t, storetest.Factory{
		Open: func(*testing.T) session.Store { return session.NewMemoryStore() },
	})
}
