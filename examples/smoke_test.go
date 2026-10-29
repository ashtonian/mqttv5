//go:build smoke

// Package examples_test runs every example program against the brokers
// conformance/docker-compose.yml starts — mosquitto on 1883, EMQX on
// 1884 with its WebSocket listener on 8083 — so an example that compiles
// but fails when run is caught:
//
//	docker compose -f conformance/docker-compose.yml up -d --wait
//	go -C examples test -tags smoke -v -run TestExamplesRun .
package examples_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// examples are the runnable programs. Those that run until interrupted
// get the interrupt after runFor and must then stop cleanly.
var examples = []string{
	"readme/quickstart", "subscribe", "typed", "request", "durable", "reconnect",
	"failover", "pool", "group", "bridge", "shared", "stats", "disconnect", "oauth", "ws",
}

const (
	runFor  = 4 * time.Second
	stopFor = 15 * time.Second
)

func TestExamplesRun(t *testing.T) {
	bin := t.TempDir()
	for _, dir := range examples {
		t.Run(dir, func(t *testing.T) {
			exe := filepath.Join(bin, filepath.Base(dir))
			if out, err := exec.Command("go", "build", "-o", exe, "./"+dir).CombinedOutput(); err != nil {
				t.Fatalf("build: %v\n%s", err, out)
			}
			var out bytes.Buffer
			cmd := exec.Command(exe)
			cmd.Env = append(os.Environ(), "MQTT_DATA_DIR="+t.TempDir())
			cmd.Stdout, cmd.Stderr = &out, &out
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()

			var err error
			select {
			case err = <-exited: // done by itself
			case <-time.After(runFor):
				_ = cmd.Process.Signal(os.Interrupt)
				select {
				case err = <-exited:
				case <-time.After(stopFor):
					_ = cmd.Process.Kill()
					<-exited
					err = errors.New("did not stop within " + stopFor.String() + " of an interrupt")
				}
			}
			if err != nil {
				t.Fatalf("%v\n%s", err, out.String())
			}
		})
	}
}
