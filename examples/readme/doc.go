// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package readme holds every Go snippet of the repository README in a
// form that compiles, so the README cannot drift from the API:
// TestREADMESnippets fails when a README snippet is not found, line for
// line, in one of these files. The functions are never called.
package readme

import (
	"context"

	"github.com/ashtonian/mqttv5"
)

var (
	ctx    = context.Background()
	cli    *mqttv5.Client
	g      *mqttv5.ClientGroup
	p      []byte
	data   []byte
	url    string
	broker string
	token1 string
	token2 string
)

func handle(*mqttv5.Message)  {}
func process(*mqttv5.Message) {}
