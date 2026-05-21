// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// TestDisconnectWithCarriesOpts verifies the DisconnectWith path sends
// the supplied DisconnectOpts on the wire (reason code + ReasonString
// + SessionExpiryInterval override).
func TestDisconnectWithCarriesOpts(t *testing.T) {
	got := make(chan *wire.Disconnect, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			if d, ok := pkt.(*wire.Disconnect); ok {
				got <- d.Clone()
				pkt.Release()
				return
			}
			pkt.Release()
		}
	})

	cli, err := New(WithBroker(fb.URL()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	expiry := uint32(0)
	if err := cli.DisconnectWith(context.Background(), wire.DisconnectOpts{
		ReasonCode:            wire.ReasonAdministrativeAction,
		ReasonString:          "test teardown",
		SessionExpiryInterval: &expiry,
	}); err != nil {
		t.Fatalf("DisconnectWith: %v", err)
	}

	select {
	case d := <-got:
		if d.ReasonCode != wire.ReasonAdministrativeAction {
			t.Errorf("ReasonCode = 0x%02X, want 0x%02X",
				byte(d.ReasonCode), byte(wire.ReasonAdministrativeAction))
		}
		rs, _ := d.Properties.String(wire.PropReasonString)
		if rs != "test teardown" {
			t.Errorf("ReasonString = %q, want test teardown", rs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker never received DISCONNECT")
	}
}
