// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package trie

import (
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

func TestTrieClone(t *testing.T) {
	orig := NewTree()
	orig.Register("a/+", 1)
	orig.Register("b/#", 2)

	clone := orig.Clone()
	if got := collect(clone, "a/x"); !sliceEq(got, []int{1}) {
		t.Errorf("clone missing 1: got %v", got)
	}

	// Mutating the clone must not affect the original.
	clone.Register("c/d", 3)
	if got := collect(orig, "c/d"); got != nil {
		t.Errorf("original mutated by clone: got %v", got)
	}
	if got := collect(clone, "c/d"); !sliceEq(got, []int{3}) {
		t.Errorf("clone missing 3: got %v", got)
	}
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
