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
