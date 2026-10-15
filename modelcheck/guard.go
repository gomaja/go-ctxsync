// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import "fmt"

// The model uses three-bit ticket counters with at most four outstanding
// claims. Relative-counter normalization removes absolute progress while
// preserving wrap, ticket equality, and stale Try snapshots. The product's
// counter width changes the period, not the transition arithmetic.
const guardMask uint8 = 7

type guardPhase uint8

const (
	guardAllocate guardPhase = iota
	guardPoll
	guardDecide
	guardYield
	guardBody
	guardUnlock
	guardUnlockLoad
	guardUnlockCAS
	guardTryLoad
	guardTryCAS
	guardDone
	guardSlotLoad
	guardRecheck
	guardSlotPoll
	guardSlotYield
	guardNotify
	guardNotifyBeforeCAS
	guardRecheckFirst
)

type guardPollingPolicy uint8

const (
	guardProductPolicy guardPollingPolicy = iota
	guardConservativePolicy
	guardNotifyBeforeRelease
	guardOmitRecheck
	guardRecheckBeforeSlot
	guardNotifyWrongTicket
	guardNotifyTooFew
)

type guardActor struct {
	PC                          guardPhase
	Ticket, SeenNext, SeenHead  uint8
	SeenSlot, NotifyTicket      uint8
	Claimed, Owns, Ready, Ahead bool
	CollisionUsed               bool
}

type guardState struct {
	Next, Serving uint8
	Slots         [8]uint8
	G             [4]guardActor
}

type guardConfig struct {
	Actors      int
	Try, Repeat [4]bool
	Policy      guardPollingPolicy
	Collisions  bool
}

type guardEdge struct {
	Next  guardState
	Actor int
	Note  string
}

func guardInitial(c guardConfig) guardState {
	s := guardState{}
	for i := range s.G {
		if i >= c.Actors {
			s.G[i].PC = guardDone
		} else if c.Try[i] {
			s.G[i].PC = guardTryLoad
		}
	}
	return s
}

// Slot counters use all eight bits. Subtracting each slot's current value
// from its saved observations preserves equality without reducing the period.
// At most three delayed releases and three releases preceding a waiting claim
// can notify its slot, plus one optional collision per sample: fewer than 256.
func guardObservesSlot(pc guardPhase) bool {
	return pc == guardRecheck || pc == guardSlotPoll || pc == guardSlotYield
}

func guardNormalize(s guardState) guardState {
	shift := s.Serving
	s.Serving = 0
	s.Next = (s.Next - shift) & guardMask
	for i := range s.G {
		g := &s.G[i]
		if guardObservesSlot(g.PC) {
			g.SeenSlot -= s.Slots[g.Ticket]
		} else {
			g.SeenSlot, g.CollisionUsed = 0, false
		}
		if g.PC == guardNotify {
			g.NotifyTicket = (g.NotifyTicket - shift) & guardMask
		} else {
			g.NotifyTicket = 0
		}
		if g.Claimed {
			g.Ticket = (g.Ticket - shift) & guardMask
		} else {
			g.Ticket = 0
		}
		if g.PC == guardTryCAS || g.PC == guardUnlockCAS || g.PC == guardNotifyBeforeCAS {
			g.SeenHead = (g.SeenHead - shift) & guardMask
			g.SeenNext = (g.SeenNext - shift) & guardMask
		} else {
			g.SeenHead, g.SeenNext = 0, 0
		}
		if g.PC != guardDecide {
			g.Ready, g.Ahead = false, false
		}
	}
	// Current values become zero, so rotation of the ticket-indexed slots
	// needs no array copy. Saved observations and pending targets rotate above.
	s.Slots = [8]uint8{}
	return s
}

func guardSuccessors(s guardState, c guardConfig, unsafeTry bool) []guardEdge {
	var out []guardEdge
	for i := 0; i < c.Actors; i++ {
		g := s.G[i]
		if g.PC == guardDone {
			continue
		}
		n := s
		p := &n.G[i]
		note := ""
		complete := func() {
			n.G[i] = guardActor{PC: guardDone}
			if c.Repeat[i] {
				if c.Try[i] {
					n.G[i].PC = guardTryLoad
				} else {
					n.G[i].PC = guardAllocate
				}
			}
		}
		switch g.PC {
		case guardAllocate:
			p.Ticket = n.Next
			n.Next = (n.Next + 1) & guardMask
			p.Claimed = true
			p.PC = guardPoll
			note = "tail Add allocates unique ticket"
		case guardPoll:
			p.Ready = n.Serving == g.Ticket
			// Keep the branch observation from this Load, even if the head
			// advances before Decide. The historical graph forgets this
			// unused bit to retain its original state counts.
			p.Ahead = c.Policy != guardConservativePolicy && !p.Ready && (g.Ticket-n.Serving-1)&guardMask != 0
			p.PC = guardDecide
			note = "head Load"
		case guardDecide:
			if g.Ready {
				p.Owns = true
				p.PC = guardBody
			} else {
				p.PC = guardYield
				if g.Ahead {
					p.PC = guardSlotLoad
					if c.Policy == guardRecheckBeforeSlot {
						p.PC = guardRecheckFirst
					}
				}
			}
			note = "ticket equality branch"
		case guardYield:
			p.PC = guardPoll
			note = "Gosched"
		case guardSlotLoad:
			p.SeenSlot = n.Slots[g.Ticket]
			p.CollisionUsed = false
			p.PC = guardRecheck
			if c.Policy == guardOmitRecheck || c.Policy == guardRecheckBeforeSlot {
				p.PC = guardSlotPoll
			}
			note = "slot Load remembers notification value"
		case guardRecheck:
			p.PC = guardSlotPoll
			if (g.Ticket-n.Serving)&guardMask <= 1 {
				p.PC = guardPoll
			}
			note = "head Load rechecks whether claim is now next or head"
		case guardRecheckFirst:
			p.PC = guardSlotLoad
			if (g.Ticket-n.Serving)&guardMask <= 1 {
				p.PC = guardPoll
			}
			note = "broken head recheck before slot Load"
		case guardSlotPoll:
			p.PC = guardSlotYield
			if n.Slots[g.Ticket] != g.SeenSlot {
				p.PC = guardPoll
			}
			note = "slot Load compares remembered value"
		case guardSlotYield:
			p.PC = guardSlotPoll
			note = "Gosched while watching slot"
		case guardNotify:
			n.Slots[g.NotifyTicket]++
			complete()
			note = "slot Add notifies newHead+1 after release CAS"
		case guardNotifyBeforeCAS:
			n.Slots[(g.SeenHead+2)&guardMask]++
			p.PC = guardUnlockCAS
			note = "broken slot Add before release CAS"
		case guardBody:
			if unsafeTry {
				p.PC = guardUnlock
			} else {
				p.PC = guardUnlockLoad
			}
			note = "finite guarded body"
		case guardUnlock:
			n.Serving = (n.Serving + 1) & guardMask
			complete()
			note = "head advances"
		case guardUnlockLoad:
			p.SeenHead, p.SeenNext, p.PC = n.Serving, n.Next, guardUnlockCAS
			if c.Policy == guardNotifyBeforeRelease && (n.Next-n.Serving-1)&guardMask > 1 {
				p.PC = guardNotifyBeforeCAS
			}
			note = "packed head/tail Load for Unlock"
		case guardUnlockCAS:
			if n.Next == g.SeenNext && n.Serving == g.SeenHead {
				n.Serving = (n.Serving + 1) & guardMask
				remaining := (g.SeenNext - n.Serving) & guardMask
				threshold := uint8(1)
				if c.Policy == guardNotifyTooFew {
					threshold = 2
				}
				if c.Policy != guardConservativePolicy && c.Policy != guardNotifyBeforeRelease && remaining > threshold {
					target := (n.Serving + 1) & guardMask
					if c.Policy == guardNotifyWrongTicket {
						target = (target + 1) & guardMask
					}
					*p = guardActor{PC: guardNotify, NotifyTicket: target}
				} else {
					complete()
				}
			} else {
				p.PC = guardUnlockLoad
			}
			note = "packed CAS advances head without carrying into tail"
		case guardTryLoad:
			p.SeenHead = n.Serving
			p.SeenNext = n.Next
			p.PC = guardTryCAS
			note = "packed head/tail Load for Try"
		case guardTryCAS:
			match := n.Next == g.SeenNext && n.Serving == g.SeenHead && g.SeenHead == g.SeenNext
			if unsafeTry {
				match = n.Next == g.SeenHead
			}
			if match {
				p.Ticket = g.SeenHead
				n.Next = (n.Next + 1) & guardMask
				p.Claimed, p.Owns, p.PC = true, true, guardBody
			} else {
				complete()
			}
			note = "Try compare and swap"
		}
		out = append(out, guardEdge{Next: guardNormalize(n), Actor: i, Note: note})
		if g.PC == guardDecide && !g.Ready && (c.Policy == guardConservativePolicy || !g.Ahead) {
			// Only the next claim may continue directly to a fresh head
			// Load. Its finite poll budget is conservatively unbounded.
			p.PC = guardPoll
			out = append(out, guardEdge{Next: guardNormalize(n), Actor: i, Note: "mismatched head may be polled again before yield"})
		}
		if g.PC == guardSlotPoll && s.Slots[g.Ticket] == g.SeenSlot {
			p.PC = guardSlotPoll
			out = append(out, guardEdge{Next: guardNormalize(n), Actor: i, Note: "unchanged slot may be polled again before yield"})
		}
	}
	if c.Collisions {
		for i := 0; i < c.Actors; i++ {
			g := s.G[i]
			if !guardObservesSlot(g.PC) || g.CollisionUsed {
				continue
			}
			n := s
			n.Slots[g.Ticket]++
			n.G[i].CollisionUsed = true
			out = append(out, guardEdge{Next: guardNormalize(n), Actor: -1, Note: "hash collision increments watched slot"})
		}
	}
	return out
}

func guardInvariant(s guardState) string {
	owners, claims := 0, 0
	var tickets uint8
	for _, g := range s.G {
		if guardObservesSlot(g.PC) {
			if !g.Claimed || g.Owns {
				return "P1: slot observer must be a waiting claim"
			}
			// Normalization preserves the number of increments since the
			// sample. Seven is a conservative bound for four actors and one
			// environmental collision, well below the uint8 ABA period.
			if s.Slots[g.Ticket]-g.SeenSlot > 7 {
				return "P1: slot observation exceeds notification bound"
			}
		} else if g.SeenSlot != 0 || g.CollisionUsed {
			return "P1: dead slot observation retained"
		}
		if g.PC == guardNotify && (g.Claimed || g.Owns) {
			return "P1: notification must follow ownership release"
		}
		if g.PC != guardNotify && g.NotifyTicket != 0 {
			return "P1: dead notification target retained"
		}
		if g.PC != guardDecide && (g.Ready || g.Ahead) {
			return "P1: dead head observation retained"
		}
		if g.Owns {
			owners++
			if !g.Claimed || g.Ticket != s.Serving {
				return "P1: guard owner does not hold head ticket"
			}
		}
		if g.Claimed {
			claims++
			bit := uint8(1) << g.Ticket
			if tickets&bit != 0 {
				return "P1: duplicate outstanding guard ticket"
			}
			tickets |= bit
		}
	}
	if owners > 1 {
		return "P1: overlapping guard owners"
	}
	if claims != int((s.Next-s.Serving)&guardMask) {
		return "P1: guard counters differ from outstanding claims"
	}
	return ""
}

func guardExplore(c guardConfig, unsafeTry bool) (int, int, string, []guardEdge) {
	type node struct {
		State  guardState
		Parent int
		Edge   guardEdge
	}
	start := guardInitial(c)
	nodes := []node{{State: start, Parent: -1}}
	seen := map[guardState]uint32{start: 0}
	edges := 0
	graph := fairGraph{Actors: c.Actors}
	for at := 0; at < len(nodes); at++ {
		state := nodes[at].State
		graphAppend(&graph, state.G[0].PC != guardDone, state.G[0].Owns, true, 0)
		successors := guardSuccessors(nodes[at].State, c, unsafeTry)
		if len(successors) == 0 {
			for _, actor := range nodes[at].State.G {
				if actor.PC != guardDone {
					return len(nodes), edges, "pending guard dead end", nil
				}
			}
		}
		for _, e := range successors {
			edges++
			if problem := guardInvariant(e.Next); problem != "" {
				var reverse []guardEdge
				reverse = append(reverse, e)
				for from := at; nodes[from].Parent != -1; from = nodes[from].Parent {
					reverse = append(reverse, nodes[from].Edge)
				}
				path := make([]guardEdge, len(reverse))
				for i := range reverse {
					path[len(reverse)-1-i] = reverse[i]
				}
				return len(nodes), edges, problem, path
			}
			to, ok := seen[e.Next]
			if !ok {
				to = uint32(len(nodes))
				seen[e.Next] = to
				nodes = append(nodes, node{State: e.Next, Parent: at, Edge: e})
			}
			if e.Actor >= 0 {
				graph.Enabled[at] |= 1 << uint(e.Actor)
			}
			graph.Edges[at] = append(graph.Edges[at], fairEdge{To: to, Actor: e.Actor, Progress: e.Actor >= 0})
		}
	}
	r, err := findFairCycle(graph)
	if err != nil || !r.Exhaustive {
		return len(nodes), edges, fmt.Sprintf("guard fairness search incomplete: %v", err), nil
	}
	if r.Cycle != nil {
		return len(nodes), edges, "fair guard starvation", nil
	}
	return len(nodes), edges, "", nil
}

func guardLiveness(c guardConfig, unsafeTry bool) (int, int, *fairCycle, error) {
	nodes, g, edges := guardLivenessGraph(c, unsafeTry)
	for i, s := range nodes {
		if problem := guardInvariant(s); problem != "" {
			return len(nodes), edges, nil, fmt.Errorf("state %d: %s", i, problem)
		}
		if g.VictimActive[i] && !g.VictimOwns[i] && len(g.Edges[i]) == 0 {
			return len(nodes), edges, nil, fmt.Errorf("pending dead end at state %d", i)
		}
	}
	f, err := findFairCycle(g)
	return len(nodes), edges, f.Cycle, err
}

func guardLivenessGraph(c guardConfig, unsafeTry bool) ([]guardState, fairGraph, int) {
	start := guardInitial(c)
	nodes := []guardState{start}
	seen := map[guardState]uint32{start: 0}
	g := fairGraph{Actors: c.Actors}
	edges := 0
	for at := 0; at < len(nodes); at++ {
		s := nodes[at]
		graphAppend(&g, s.G[0].PC != guardDone, s.G[0].Owns, true, 0)
		if s.G[0].Owns || s.G[0].PC == guardDone {
			continue
		}
		for _, e := range guardSuccessors(s, c, unsafeTry) {
			edges++
			to, ok := seen[e.Next]
			if !ok {
				to = uint32(len(nodes))
				seen[e.Next] = to
				nodes = append(nodes, e.Next)
			}
			if e.Actor >= 0 {
				g.Enabled[at] |= 1 << uint(e.Actor)
			}
			g.Edges[at] = append(g.Edges[at], fairEdge{To: to, Actor: e.Actor, Progress: e.Actor >= 0})
		}
	}
	return nodes, g, edges
}
