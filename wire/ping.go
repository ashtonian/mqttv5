// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import "io"

// Pingreq is a PINGREQ packet (MQTT v5 §3.12). Empty body.
type Pingreq struct{}

// Type implements Packet.
func (*Pingreq) Type() PacketType { return PINGREQ }

// Release is a no-op — Pingreq is a singleton with no pooled state.
func (*Pingreq) Release() {}

// Pingresp is a PINGRESP packet (MQTT v5 §3.13). Empty body.
type Pingresp struct{}

// Type implements Packet.
func (*Pingresp) Type() PacketType { return PINGRESP }

// Release is a no-op — Pingresp is a singleton with no pooled state.
func (*Pingresp) Release() {}

// Single shared instances. Decoder returns these unchanged; their zero
// state is correct.
var (
	pingreqInstance  = &Pingreq{}
	pingrespInstance = &Pingresp{}
)

// decodePingreq returns the shared PINGREQ singleton. The frame buffer
// is empty for PINGREQ (RemainingLength == 0) so we hand it straight
// back to the pool.
func decodePingreq(frame *[]byte, _ byte) (*Pingreq, error) {
	releaseBuf(frame)
	return pingreqInstance, nil
}

func decodePingresp(frame *[]byte, _ byte) (*Pingresp, error) {
	releaseBuf(frame)
	return pingrespInstance, nil
}

// WritePingreq emits a PINGREQ packet (2 bytes: type byte + 0 remaining
// length).
func WritePingreq(w io.Writer) (int64, error) {
	n, err := w.Write([]byte{byte(PINGREQ) << 4, 0x00})
	return int64(n), err
}

// WritePingresp emits a PINGRESP packet (2 bytes).
func WritePingresp(w io.Writer) (int64, error) {
	n, err := w.Write([]byte{byte(PINGRESP) << 4, 0x00})
	return int64(n), err
}
