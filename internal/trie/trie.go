// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package trie implements an MQTT v5 topic filter matcher used by the
// client to dispatch inbound PUBLISH packets to subscribed handlers.
//
// Wildcards (per §4.7):
//
//   - +  matches exactly one level
//   - #  matches zero or more trailing levels (only valid as the final level)
//
// Register and Unregister change the tree in place in O(depth) — the
// cost does not depend on how many filters are registered — and
// Unregister prunes the nodes it empties. Match takes a read lock, so
// yield must not call back into the tree.
package trie

import (
	"strings"
	"sync"
)

// Handler is the registered callback. The trie does not interpret it;
// it just carries the value through to Match.
type Handler any

// Node is one level in the trie.
type Node struct {
	children  map[string]*Node
	plusChild *Node
	hashChild *Node // # terminal — handlers here match everything from this point on
	handlers  []entry
}

func (n *Node) empty() bool {
	return len(n.children) == 0 && n.plusChild == nil && n.hashChild == nil && len(n.handlers) == 0
}

type entry struct {
	id      uint64 // unique within the Tree, for Unregister
	handler Handler
}

// Tree is a topic filter trie, safe for concurrent use.
type Tree struct {
	mu     sync.RWMutex
	root   *Node
	nextID uint64
}

// NewTree returns an empty Tree.
func NewTree() *Tree { return &Tree{root: &Node{}} }

// Register adds h under the given topic filter and returns an opaque
// id. Pass that id to Unregister to remove this specific registration
// (multiple handlers can share a filter).
func (t *Tree) Register(filter string, h Handler) (id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID++
	id = t.nextID
	node := t.ensure(filter)
	node.handlers = append(node.handlers, entry{id: id, handler: h})
	return id
}

// Unregister removes the handler registered with id under filter and
// prunes nodes left empty. It reports whether the handler was found.
func (t *Tree) Unregister(filter string, id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	path := t.path(filter)
	if path == nil {
		return false
	}
	leaf := path[len(path)-1].node
	i := -1
	for j, e := range leaf.handlers {
		if e.id == id {
			i = j
			break
		}
	}
	if i < 0 {
		return false
	}
	leaf.handlers = append(leaf.handlers[:i], leaf.handlers[i+1:]...)
	if len(leaf.handlers) == 0 {
		leaf.handlers = nil
	}
	// Remove empty nodes bottom-up; the root stays.
	for k := len(path) - 1; k > 0 && path[k].node.empty(); k-- {
		parent, step := path[k-1].node, path[k]
		switch step.level {
		case "+":
			parent.plusChild = nil
		case "#":
			parent.hashChild = nil
		default:
			delete(parent.children, step.level)
		}
	}
	return true
}

// Reset removes every registration.
func (t *Tree) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.root = &Node{}
}

// Nodes reports the number of nodes, root included.
func (t *Tree) Nodes() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return countNodes(t.root)
}

func countNodes(n *Node) int {
	if n == nil {
		return 0
	}
	c := 1 + countNodes(n.plusChild) + countNodes(n.hashChild)
	for _, ch := range n.children {
		c += countNodes(ch)
	}
	return c
}

// Match walks the tree for topic, invoking yield with every handler
// whose filter matches. It holds the read lock throughout: yield must
// not call Register, Unregister or Reset.
//
// Zero-allocation: walks topic in place via byte indexing for '/'
// rather than allocating a []string of levels. The substring slicing
// is zero-copy; the map[string]*Node lookups with substring keys are
// also zero-alloc thanks to the compiler-special-cased string-key
// optimization.
//
// A topic starting with '$' (e.g. $SYS/...) is not matched by a filter
// whose first level is a wildcard [MQTT-4.7.2-1]: "#" and "+/x" do not
// receive $SYS traffic, "$SYS/#" does.
func (t *Tree) Match(topic string, yield func(Handler)) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if strings.HasPrefix(topic, "$") {
		matchDollar(t.root, topic, yield)
		return
	}
	matchLevels(t.root, topic, 0, yield)
}

// matchDollar matches a '$' topic: its first level only matches
// literally, deeper levels follow the normal rules.
func matchDollar(root *Node, topic string, yield func(Handler)) {
	level, next := topic, len(topic)+1
	if i := strings.IndexByte(topic, '/'); i >= 0 {
		level, next = topic[:i], i+1
	}
	if child, ok := root.children[level]; ok {
		matchLevels(child, topic, next, yield)
	}
}

// matchLevels recurses down the trie, slicing the next level out of
// topic via IndexByte('/') without allocating a []string.
func matchLevels(n *Node, topic string, start int, yield func(Handler)) {
	if n == nil {
		return
	}
	// # at the current node matches every remaining suffix, including
	// the empty suffix. (Spec §4.7.1.2: "the multi-level wildcard
	// represents the parent and any number of child levels".)
	if n.hashChild != nil {
		for _, e := range n.hashChild.handlers {
			yield(e.handler)
		}
	}

	// Past end of topic — emit handlers registered at this exact node.
	// start > len(topic) signals "we already consumed every level
	// (including the trailing empty if topic ended with '/')."
	if start > len(topic) {
		for _, e := range n.handlers {
			yield(e.handler)
		}
		return
	}

	// Carve out the next level. Topic "a/b/c" with start=0 yields
	// level="a", next=2 (after the '/'). Empty topic gives level=""
	// once with next=1, mirroring the previous strings.Split behaviour.
	var level string
	var next int
	if rel := strings.IndexByte(topic[start:], '/'); rel < 0 {
		level = topic[start:]
		next = len(topic) + 1 // sentinel: past end
	} else {
		level = topic[start : start+rel]
		next = start + rel + 1
	}

	if n.plusChild != nil {
		matchLevels(n.plusChild, topic, next, yield)
	}
	if child, ok := n.children[level]; ok {
		matchLevels(child, topic, next, yield)
	}
}

// ensure walks/creates trie nodes for filter and returns the leaf node
// that owns this filter's handler slot.
func (t *Tree) ensure(filter string) *Node {
	levels := splitLevels(filter)
	node := t.root
	for _, level := range levels {
		switch level {
		case "+":
			if node.plusChild == nil {
				node.plusChild = &Node{}
			}
			node = node.plusChild
		case "#":
			// # is terminal — per spec it's only allowed at the end.
			// Caller validates; we defensively return the hash child
			// so handler registration doesn't silently disappear even
			// if the caller passed a malformed filter like "a/#/b".
			// Any levels after # are ignored.
			if node.hashChild == nil {
				node.hashChild = &Node{}
			}
			return node.hashChild
		default:
			if node.children == nil {
				node.children = make(map[string]*Node)
			}
			child, ok := node.children[level]
			if !ok {
				child = &Node{}
				node.children[level] = child
			}
			node = child
		}
	}
	return node
}

// step is one node on the path to a filter and the level that led to it.
type step struct {
	level string
	node  *Node
}

// path returns the nodes from the root to filter's node, or nil when the
// filter has no node.
func (t *Tree) path(filter string) []step {
	levels := splitLevels(filter)
	path := make([]step, 1, len(levels)+1)
	path[0] = step{node: t.root}
	node := t.root
	for _, level := range levels {
		switch level {
		case "+":
			node = node.plusChild
		case "#":
			node = node.hashChild
		default:
			node = node.children[level]
		}
		if node == nil {
			return nil
		}
		path = append(path, step{level: level, node: node})
		if level == "#" {
			break
		}
	}
	return path
}

// splitLevels is a thin wrapper over strings.Split for consistency.
// MQTT topic levels are separated by '/'; an empty filter is treated
// as a single empty level (which can match an empty topic).
func splitLevels(s string) []string { return strings.Split(s, "/") }
