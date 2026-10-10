// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

const (
	envCrashDir    = "MQTTV5_CRASH_DIR"
	envCrashBroker = "MQTTV5_CRASH_BROKER"
	crashClientID  = "crash-recovery-client"
	inboundID      = 50
)

// TestStoreCrashRecovery kills a client process holding unfinished QoS
// 1/2 state and checks that a new process with the same store resumes
// it: QoS 1 publishes are resent with DUP=1 in their original order, a
// QoS 2 publish that got its PUBREC resumes with PUBREL, and an inbound
// QoS 2 message whose PUBREC was sent answers the broker's PUBREL with
// PUBCOMP 0x00 instead of 0x92.
func TestStoreCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a child process")
	}
	dir := t.TempDir()
	type firstRun struct {
		qos1  []uint16
		qos2  uint16
		clean bool
	}
	crashPoint := make(chan firstRun, 1)
	type resumed struct {
		clean   bool
		order   []string
		pubcomp wire.ReasonCode
		dups    bool
	}
	result := make(chan resumed, 1)

	b := testbroker.New(t,
		// The child process: hold everything unacknowledged, then signal.
		func(c *testbroker.Conn) {
			ci := c.AcceptConnect(wire.ConnackOpts{})
			c.ServeSubscribe(-1)
			c.Publish(wire.PublishOpts{Topic: "in/x", Payload: []byte("inbound"), QoS: 2, PacketID: inboundID})
			var run firstRun
			run.clean = ci != nil && ci.CleanStart
			gotPubrec, gotPubrel := false, false
			for !gotPubrec || !gotPubrel || len(run.qos1) < 3 {
				p, ok, err := c.Next(10 * time.Second)
				if !ok {
					c.T.Errorf("first run stalled: %v (qos1 %v pubrec %v pubrel %v)", err, run.qos1, gotPubrec, gotPubrel)
					return
				}
				switch {
				case p.Type == wire.PUBLISH && p.QoS == 1:
					run.qos1 = append(run.qos1, p.PacketID)
				case p.Type == wire.PUBLISH && p.QoS == 2:
					run.qos2 = p.PacketID
					c.Pubrec(p.PacketID, wire.ReasonSuccess)
				case p.Type == wire.PUBREC && p.PacketID == inboundID:
					gotPubrec = true
				case p.Type == wire.PUBREL:
					gotPubrel = true
				}
			}
			crashPoint <- run
			c.Hold(0)
		},
		// The restarted client.
		func(c *testbroker.Conn) {
			ci := c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			var r resumed
			r.clean = ci == nil || ci.CleanStart
			r.dups = true
			var acks []func()
			for len(r.order) < 4 {
				p, ok, _ := c.Next(5 * time.Second)
				if !ok {
					break
				}
				id := p.PacketID
				switch p.Type {
				case wire.PUBLISH:
					r.order = append(r.order, fmt.Sprintf("PUBLISH#%d", id))
					r.dups = r.dups && p.Dup
					acks = append(acks, func() { c.Puback(id, wire.ReasonSuccess) })
				case wire.PUBREL:
					r.order = append(r.order, fmt.Sprintf("PUBREL#%d", id))
					acks = append(acks, func() { c.Pubcomp(id, wire.ReasonSuccess) })
				}
			}
			c.Pubrel(inboundID, wire.ReasonSuccess)
			if p, ok := c.Await(wire.PUBCOMP, 5*time.Second); ok {
				r.pubcomp = p.Reason
			} else {
				r.pubcomp = 0xff
			}
			for _, ack := range acks {
				ack()
			}
			result <- r
			c.ServeAuto()
		},
	)

	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.v")
	cmd.Env = append(os.Environ(), envCrashDir+"="+dir, envCrashBroker+"="+b.URL())
	cmd.Stdout, cmd.Stderr = testWriter{t}, testWriter{t}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var run firstRun
	select {
	case run = <-crashPoint:
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("child never reached the crash point")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if !run.clean {
		t.Error("first run did not start a clean session")
	}

	st, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer st.Close()
	cli, err := mqttv5.New(
		mqttv5.WithBroker(b.URL()),
		mqttv5.WithClientID(crashClientID),
		mqttv5.WithStore(st),
		mqttv5.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	r := <-result
	if r.clean {
		t.Error("restarted client sent CleanStart=1 although the store held a session")
	}
	want := []string{fmt.Sprintf("PUBREL#%d", run.qos2)}
	for _, id := range run.qos1 {
		want = append(want, fmt.Sprintf("PUBLISH#%d", id))
	}
	if !slices.Equal(r.order, want) {
		t.Errorf("resumed with %v, want %v", r.order, want)
	}
	if !r.dups {
		t.Error("resent PUBLISH without DUP=1")
	}
	if r.pubcomp != wire.ReasonSuccess {
		t.Errorf("PUBCOMP for the stored inbound QoS 2 message = %#x, want 0x00", byte(r.pubcomp))
	}

	// The broker acknowledged everything; the store must end up empty.
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, recs, _ := st.Load(ctx); len(recs) == 0 {
			break
		}
	}
	if err := cli.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, recs, _ := st.Load(ctx); len(recs) != 0 {
		t.Fatalf("records left after every flow completed: %+v", recs)
	}
}

// TestCrashChild is the child process of TestStoreCrashRecovery.
func TestCrashChild(t *testing.T) {
	dir, url := os.Getenv(envCrashDir), os.Getenv(envCrashBroker)
	if dir == "" {
		t.Skip("helper process for TestStoreCrashRecovery")
	}
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := mqttv5.New(
		mqttv5.WithBroker(url),
		mqttv5.WithClientID(crashClientID),
		mqttv5.WithStore(st),
		mqttv5.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "in/#", QoS: 2}})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for m := range ch {
			_ = m.Ack()
		}
	}()
	for i := 0; i < 3; i++ {
		go func() { _ = cli.Publish(ctx, mqttv5.PublishOptions{Topic: "q1", Payload: []byte{byte(i)}, QoS: 1}) }()
	}
	go func() { _ = cli.Publish(ctx, mqttv5.PublishOptions{Topic: "q2", Payload: []byte("two"), QoS: 2}) }()
	select {} // killed by the parent
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("child: %s", p)
	return len(p), nil
}
