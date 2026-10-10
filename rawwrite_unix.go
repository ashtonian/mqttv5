// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build unix

package mqttv5

import (
	"errors"
	"os"
	"syscall"

	"github.com/ashtonian/mqttv5/transport"
)

// rawWriter writes to a socket without waiting for room in its send
// buffer. A write deadline would bound the wait too, but arming its timer
// wakes a thread of the runtime's network poller on every write. It is
// guarded by the connection's wmu.
type rawWriter struct {
	rc    syscall.RawConn
	write func(fd uintptr) bool // built once: a closure per call allocates
	buf   []byte
	n     int
	err   error
}

// newRawWriter returns a rawWriter for conn, or nil when conn has no
// socket to write to.
func newRawWriter(conn transport.Conn) *rawWriter {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return nil
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return nil
	}
	w := &rawWriter{rc: rc}
	w.write = func(fd uintptr) bool {
		for {
			w.n, w.err = syscall.Write(int(fd), w.buf)
			if !errors.Is(w.err, syscall.EINTR) {
				return true
			}
		}
	}
	return w
}

// tryWrite writes as much of b as the socket takes now and returns how
// much that was.
func (w *rawWriter) tryWrite(b []byte) (int, error) {
	w.buf = b
	err := w.rc.Write(w.write)
	n, werr := w.n, w.err
	w.buf, w.err = nil, nil
	switch {
	case err != nil:
		return 0, err
	case errors.Is(werr, syscall.EAGAIN):
		return 0, nil
	case werr != nil:
		return 0, os.NewSyscallError("write", werr)
	}
	return n, nil
}
