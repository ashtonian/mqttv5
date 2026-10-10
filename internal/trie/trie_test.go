// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package trie

import (
	"fmt"
	"sort"
	"testing"
)

// collect runs Match and returns the handler IDs in sorted order so
// tests can assert without depending on map iteration order.
func collect(tree *Tree, topic string) []int {
	var got []int
	tree.Match(topic, func(h Handler) {
		got = append(got, h.(int))
	})
	sort.Ints(got)
	return got
}

func TestTrieExactMatch(t *testing.T) {
	tree := NewTree()
	tree.Register("sport/tennis/player1", 1)
	tree.Register("sport/tennis", 2)
	tree.Register("sport", 3)

	for _, tc := range []struct {
		topic string
		want  []int
	}{
		{"sport/tennis/player1", []int{1}},
		{"sport/tennis", []int{2}},
		{"sport", []int{3}},
		{"sport/tennis/player2", nil},
		{"unrelated/topic", nil},
	} {
		if got := collect(tree, tc.topic); !sliceEq(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.topic, got, tc.want)
		}
	}
}

func TestTriePlusWildcard(t *testing.T) {
	tree := NewTree()
	tree.Register("sport/+/player1", 1)
	tree.Register("+/tennis/player1", 2)
	tree.Register("+/+/+", 3)

	for _, tc := range []struct {
		topic string
		want  []int
	}{
		{"sport/tennis/player1", []int{1, 2, 3}},
		{"sport/football/player1", []int{1, 3}},
		{"other/tennis/player1", []int{2, 3}},
		// + matches exactly one level — too many levels = no match for "+/+/+"
		{"a/b/c/d", nil},
		// + cannot match zero levels
		{"sport/tennis", nil},
	} {
		if got := collect(tree, tc.topic); !sliceEq(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.topic, got, tc.want)
		}
	}
}

func TestTrieHashWildcard(t *testing.T) {
	tree := NewTree()
	tree.Register("sport/#", 1)
	tree.Register("#", 2)
	tree.Register("sport/tennis/#", 3)

	for _, tc := range []struct {
		topic string
		want  []int
	}{
		// # matches parent + any sub-levels (§4.7.1.2):
		//   sport/#         matches "sport" and "sport/..."
		//   sport/tennis/#  matches "sport/tennis" and "sport/tennis/..."
		{"sport", []int{1, 2}},
		{"sport/tennis", []int{1, 2, 3}},
		{"sport/tennis/player1", []int{1, 2, 3}},
		{"sport/tennis/player1/score", []int{1, 2, 3}},
		// # at root matches everything
		{"unrelated", []int{2}},
		{"a/b/c/d/e", []int{2}},
	} {
		if got := collect(tree, tc.topic); !sliceEq(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.topic, got, tc.want)
		}
	}
}

func TestTrieMultipleHandlersPerFilter(t *testing.T) {
	tree := NewTree()
	id1 := tree.Register("a/b", 1)
	id2 := tree.Register("a/b", 2)
	tree.Register("a/b", 3)

	if got := collect(tree, "a/b"); !sliceEq(got, []int{1, 2, 3}) {
		t.Fatalf("got %v, want [1 2 3]", got)
	}

	if !tree.Unregister("a/b", id1) {
		t.Fatal("Unregister id1 failed")
	}
	if got := collect(tree, "a/b"); !sliceEq(got, []int{2, 3}) {
		t.Fatalf("after remove id1: got %v, want [2 3]", got)
	}

	if !tree.Unregister("a/b", id2) {
		t.Fatal("Unregister id2 failed")
	}
	if got := collect(tree, "a/b"); !sliceEq(got, []int{3}) {
		t.Fatalf("after remove id2: got %v, want [3]", got)
	}
}

func TestTrieUnregisterMissing(t *testing.T) {
	tree := NewTree()
	if tree.Unregister("never/registered", 999) {
		t.Fatal("Unregister on missing filter returned true")
	}
}

// Unregistering prunes the nodes it empties.
func TestTriePrunes(t *testing.T) {
	tree := NewTree()
	filters := []string{"a/b/c", "a/b/+", "a/#", "+/x", "devices/1/t", "devices/2/t", "#"}
	ids := make([]uint64, len(filters))
	for i, f := range filters {
		ids[i] = tree.Register(f, i)
	}
	shared := tree.Register("a/b/c", 99)
	for i, f := range filters {
		if !tree.Unregister(f, ids[i]) {
			t.Fatalf("Unregister(%q) failed", f)
		}
	}
	if got := collect(tree, "a/b/c"); !sliceEq(got, []int{99}) {
		t.Fatalf("remaining handler: %v", got)
	}
	if !tree.Unregister("a/b/c", shared) {
		t.Fatal("Unregister shared failed")
	}
	if n := tree.Nodes(); n != 1 {
		t.Fatalf("%d nodes left after removing every filter, want only the root", n)
	}
}

// Register cost depends on the filter's depth, not on how many filters
// exist. Compare ns/op of the two sub-benchmarks.
func BenchmarkTrieRegister(b *testing.B) {
	for _, existing := range []int{100, 10000} {
		b.Run(fmt.Sprintf("existing=%d", existing), func(b *testing.B) {
			tree := NewTree()
			for i := 0; i < existing; i++ {
				tree.Register(fmt.Sprintf("devices/%d/telemetry", i), i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f := fmt.Sprintf("devices/new-%d/telemetry", i%1024)
				id := tree.Register(f, i)
				tree.Unregister(f, id)
			}
		})
	}
}

func BenchmarkTrieMatch(b *testing.B) {
	tree := NewTree()
	for i := 0; i < 10000; i++ {
		tree.Register(fmt.Sprintf("devices/%d/telemetry", i), i)
	}
	tree.Register("devices/+/telemetry", -1)
	tree.Register("#", -2)
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		tree.Match("devices/4242/telemetry", func(Handler) { n++ })
	}
	_ = n
}

func sliceEq(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// [MQTT-4.7.2-1]: wildcards at the first level do not match topics
// starting with '$'.
func TestDollarTopicsSkipLeadingWildcards(t *testing.T) {
	tr := NewTree()
	got := map[string]bool{}
	for _, f := range []string{"#", "+/x", "+/+", "$SYS/#", "$SYS/x", "$SYS/+", "+"} {
		f := f
		tr.Register(f, handlerFunc(func() { got[f] = true }))
	}
	tr.Match("$SYS/x", func(h Handler) { h.(handlerFunc)() })
	want := map[string]bool{"$SYS/#": true, "$SYS/x": true, "$SYS/+": true}
	if len(got) != len(want) {
		t.Fatalf("matched %v, want %v", got, want)
	}
	for f := range want {
		if !got[f] {
			t.Fatalf("matched %v, want %v", got, want)
		}
	}
	clear(got)
	tr.Match("$SYS", func(h Handler) { h.(handlerFunc)() })
	if !got["$SYS/#"] || got["#"] || got["+"] {
		t.Fatalf("$SYS matched %v", got)
	}
	clear(got)
	tr.Match("a/x", func(h Handler) { h.(handlerFunc)() })
	if !got["#"] || !got["+/x"] || !got["+/+"] {
		t.Fatalf("ordinary topic matched %v", got)
	}
}

type handlerFunc func()
