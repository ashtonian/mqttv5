// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	storefile "github.com/ashtonian/mqttv5/store/file"
	"github.com/ashtonian/mqttv5/wire"
)

const (
	envCrashDir    = "MQTTV5_QUEUE_CRASH_DIR"
	envCrashBroker = "MQTTV5_QUEUE_CRASH_BROKER"
	crashClientID  = "queue-crash-client"
	crashWindow    = 4
)

// crashMessages are published by the child in this order: the first
// four fill the window, the fifth waits in the queue.
var crashMessages = []struct {
	payload string
	qos     byte
}{{"a", 1}, {"b", 1}, {"c", 1}, {"d", 2}, {"e", 1}}

// TestQueueCrashRecovery kills a process whose QueuePublisher has
// exchanges in flight and checks that a new process with the same queue
// and session store continues them instead of publishing the messages
// again: the QoS 1 messages are resent with DUP=1 under their packet
// identifiers, the QoS 2 message that got its PUBREC resumes with
// PUBREL, and only the message never sent is published as new. Every
// message reaches the broker once as a new PUBLISH.
func TestQueueCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a child process")
	}
	dir := t.TempDir()
	firstIDs := make(chan map[string]uint16, 1)
	type resumed struct {
		packets []testbroker.Packet
		clean   bool
	}
	result := make(chan resumed, 1)

	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			ids := map[string]uint16{}
			for len(ids) < crashWindow {
				p, ok, err := c.Next(10 * time.Second)
				if !ok {
					c.T.Errorf("first run stalled: %v (sent %v)", err, ids)
					return
				}
				if p.Type != wire.PUBLISH {
					continue
				}
				ids[string(p.Payload)] = p.PacketID
				if p.QoS == 2 {
					c.Pubrec(p.PacketID, wire.ReasonSuccess)
					c.Expect(wire.PUBREL, 0)
				}
			}
			firstIDs <- ids
			c.Hold(0)
		},
		func(c *testbroker.Conn) {
			ci := c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			r := resumed{clean: ci == nil || ci.CleanStart}
			for {
				p, ok, _ := c.Next(time.Second)
				if !ok {
					break
				}
				switch p.Type {
				case wire.PUBLISH:
					r.packets = append(r.packets, p)
					c.Puback(p.PacketID, wire.ReasonSuccess)
				case wire.PUBREL:
					r.packets = append(r.packets, p)
					c.Pubcomp(p.PacketID, wire.ReasonSuccess)
				}
			}
			result <- r
			c.ServeAuto()
		},
	)

	cmd := exec.Command(os.Args[0], "-test.run=^TestQueueCrashChild$", "-test.v")
	cmd.Env = append(os.Environ(), envCrashDir+"="+dir, envCrashBroker+"="+b.URL())
	cmd.Stdout, cmd.Stderr = testWriter{t}, testWriter{t}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var sent map[string]uint16
	select {
	case sent = <-firstIDs:
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("child never reached the crash point")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	cli, q, st := openCrashClient(t, dir, b.URL())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	var dead []string
	pub, err := mqttv5.NewQueuePublisher(cli, q, mqttv5.WithQueueWindow(crashWindow),
		mqttv5.WithDeadLetter(func(e mqttv5.QueueEntry, err error) { dead = append(dead, string(e.Publish.Payload)) }))
	if err != nil {
		t.Fatal(err)
	}
	r := <-result
	if r.clean {
		t.Error("restarted client sent CleanStart=1 although the store held a session")
	}
	var got []string
	for _, p := range r.packets {
		switch {
		case p.Type == wire.PUBREL:
			got = append(got, fmt.Sprintf("PUBREL#%d", p.PacketID))
		case p.Dup:
			got = append(got, fmt.Sprintf("PUBLISH#%d+dup(%s)", p.PacketID, p.Payload))
		default:
			got = append(got, fmt.Sprintf("PUBLISH(%s)", p.Payload))
		}
	}
	want := []string{
		fmt.Sprintf("PUBREL#%d", sent["d"]),
		fmt.Sprintf("PUBLISH#%d+dup(a)", sent["a"]),
		fmt.Sprintf("PUBLISH#%d+dup(b)", sent["b"]),
		fmt.Sprintf("PUBLISH#%d+dup(c)", sent["c"]),
		"PUBLISH(e)",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("after the crash the broker received %q, want %q", got, want)
	}

	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if n, _ := q.Len(ctx); n == 0 {
			break
		}
	}
	if err := pub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if len(dead) != 0 {
		t.Fatalf("dead-lettered %q", dead)
	}
	if _, recs, _ := st.Load(ctx); len(recs) != 0 {
		t.Fatalf("session records left: %+v", recs)
	}
	q2, err := Open(filepath.Join(dir, "queue"))
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	if n, _ := q2.Len(ctx); n != 0 {
		t.Fatalf("%d entries left in the queue", n)
	}
}

func openCrashClient(t *testing.T, dir, url string) (*mqttv5.Client, *Queue, *storefile.Store) {
	t.Helper()
	st, err := storefile.Open(filepath.Join(dir, "session"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	q, err := Open(filepath.Join(dir, "queue"))
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
	return cli, q, st
}

// TestQueueCrashChild is the child process of TestQueueCrashRecovery.
func TestQueueCrashChild(t *testing.T) {
	dir, url := os.Getenv(envCrashDir), os.Getenv(envCrashBroker)
	if dir == "" {
		t.Skip("helper process for TestQueueCrashRecovery")
	}
	cli, q, _ := openCrashClient(t, dir, url)
	ctx := context.Background()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	pub, err := mqttv5.NewQueuePublisher(cli, q, mqttv5.WithQueueWindow(crashWindow))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range crashMessages {
		if err := pub.Publish(ctx, mqttv5.PublishOptions{Topic: "crash", QoS: m.qos, Payload: []byte(m.payload)}); err != nil {
			t.Fatal(err)
		}
	}
	select {} // killed by the parent
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("child: %s", p)
	return len(p), nil
}
