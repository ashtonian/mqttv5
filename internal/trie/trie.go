// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package trie implements an MQTT v5 topic filter matcher used by the
// client to dispatch inbound PUBLISH packets to subscribed handlers.
//
// Wildcards (per §4.7):
//
//   +  matches exactly one level
//   #  matches zero or more trailing levels (only valid as the final level)
//
// The Tree value is read-only after construction. Use Register/Unregister
// on the parent Router (see router.go in the package consumer) for
// copy-on-write updates with atomic.Pointer swap — readers do not lock.
package trie

import (
	"strings"
)

// Handler is the registered callback. The trie does not interpret it;
// it just carries the value through to Match.
type Handler any

// Node is one level in the trie. Exported for callers that want to
// build a tree by hand (e.g. tests).
type Node struct {
	children  map[string]*Node
	plusChild *Node
	hashChild *Node // # terminal — handlers here match everything from this point on
	handlers  []entry
}

type entry struct {
	id      uint64 // unique within the parent Tree, for Unregister
	handler Handler
}

// Tree is a copy-on-write topic filter trie. The zero value is an empty
// tree; use NewTree for clarity at the call site.
type Tree struct {
	root   *Node
	nextID uint64 // monotonic ID generator for entries
}

// NewTree returns an empty Tree.
func NewTree() *Tree { return &Tree{root: &Node{}} }

// Clone returns a deep copy of the tree. Used by the atomic
// copy-on-write registration path: callers Clone, mutate, then CAS.
//
// Allocations scale with total node count. Typical subscriber tables
// have on the order of 10-100 nodes; subscription mutations are rare,
// so this is the right trade-off for the lock-free Match path.
func (t *Tree) Clone() *Tree {
	return &Tree{
		root:   cloneNode(t.root),
		nextID: t.nextID,
	}
}

func cloneNode(n *Node) *Node {
	if n == nil {
		return nil
	}
	cp := &Node{
		plusChild: cloneNode(n.plusChild),
		hashChild: cloneNode(n.hashChild),
	}
	if len(n.handlers) > 0 {
		cp.handlers = append([]entry(nil), n.handlers...)
	}
	if len(n.children) > 0 {
		cp.children = make(map[string]*Node, len(n.children))
		for k, v := range n.children {
			cp.children[k] = cloneNode(v)
		}
	}
	return cp
}

// Register adds h under the given topic filter and returns an opaque
// id. Pass that id to Unregister to remove this specific registration
// (multiple handlers can share a filter).
func (t *Tree) Register(filter string, h Handler) (id uint64) {
	t.nextID++
	id = t.nextID
	node := t.ensure(filter)
	node.handlers = append(node.handlers, entry{id: id, handler: h})
	return id
}

// Unregister removes the handler registered with id under filter.
// Returns true if the handler was found and removed.
func (t *Tree) Unregister(filter string, id uint64) bool {
	node := t.find(filter)
	if node == nil {
		return false
	}
	for i, e := range node.handlers {
		if e.id == id {
			node.handlers = append(node.handlers[:i], node.handlers[i+1:]...)
			return true
		}
	}
	return false
}

// Match walks the tree for topic, invoking yield with every handler
// whose filter matches. yield should not retain Handler past return —
// the underlying entry slice may change after a subsequent CoW swap.
//
// Zero-allocation: walks topic in place via byte indexing for '/'
// rather than allocating a []string of levels. The substring slicing
// is zero-copy; the map[string]*Node lookups with substring keys are
// also zero-alloc thanks to the compiler-special-cased string-key
// optimization.
func (t *Tree) Match(topic string, yield func(Handler)) {
	matchLevels(t.root, topic, 0, yield)
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
	for i, level := range levels {
		switch level {
		case "+":
			if node.plusChild == nil {
				node.plusChild = &Node{}
			}
			node = node.plusChild
		case "#":
			if node.hashChild == nil {
				node.hashChild = &Node{}
			}
			// # is terminal — caller can register only at this node.
			if i != len(levels)-1 {
				// Per spec, # is only allowed at the end. The caller
				// should validate, but we defensively return the hash
				// child so handler registration doesn't silently
				// disappear.
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

// find walks the tree without creating nodes. Returns nil if the
// filter has no matching path.
func (t *Tree) find(filter string) *Node {
	levels := splitLevels(filter)
	node := t.root
	for _, level := range levels {
		switch level {
		case "+":
			if node.plusChild == nil {
				return nil
			}
			node = node.plusChild
		case "#":
			return node.hashChild
		default:
			if node.children == nil {
				return nil
			}
			child, ok := node.children[level]
			if !ok {
				return nil
			}
			node = child
		}
	}
	return node
}

// splitLevels is a thin wrapper over strings.Split for consistency.
// MQTT topic levels are separated by '/'; an empty filter is treated
// as a single empty level (which can match an empty topic).
func splitLevels(s string) []string { return strings.Split(s, "/") }
