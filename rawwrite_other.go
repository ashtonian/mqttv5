// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build !unix

package mqttv5

import "github.com/ashtonian/mqttv5/transport"

// rawWriter writes to a socket without waiting for room on unix only;
// elsewhere newRawWriter gives none and the writer goroutine sends
// everything.
type rawWriter struct{}

func newRawWriter(transport.Conn) *rawWriter { return nil }

func (*rawWriter) tryWrite([]byte) (int, error) { return 0, nil }
