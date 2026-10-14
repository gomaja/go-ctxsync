// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

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
)

type guardActor struct {
	PC                         guardPhase
	Ticket, SeenNext, SeenHead uint8
	Claimed, Owns, Ready       bool
}

type guardState struct {
	Next, Serving uint8
	G             [4]guardActor
}

type guardConfig struct {
	Actors      int
	Try, Repeat [4]bool
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

func guardNormalize(s guardState) guardState {
	shift := s.Serving
	s.Serving = 0
	s.Next = (s.Next - shift) & guardMask
	for i := range s.G {
		g := &s.G[i]
		if g.Claimed {
			g.Ticket = (g.Ticket - shift) & guardMask
		} else {
			g.Ticket = 0
		}
		if g.PC == guardTryCAS || g.PC == guardUnlockCAS {
			g.SeenHead = (g.SeenHead - shift) & guardMask
			g.SeenNext = (g.SeenNext - shift) & guardMask
		} else {
			g.SeenHead, g.SeenNext = 0, 0
		}
		if g.PC != guardDecide {
			g.Ready = false
		}
	}
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
			p.PC = guardDecide
			note = "head Load"
		case guardDecide:
			if g.Ready {
				p.Owns = true
				p.PC = guardBody
			} else {
				p.PC = guardYield
			}
			note = "ticket equality branch"
		case guardYield:
			p.PC = guardPoll
			note = "Gosched"
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
			note = "packed head/tail Load for Unlock"
		case guardUnlockCAS:
			if n.Next == g.SeenNext && n.Serving == g.SeenHead {
				n.Serving = (n.Serving + 1) & guardMask
				complete()
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
		if g.PC == guardDecide && !g.Ready {
			// Any finite busy-poll budget can continue directly after a
			// mismatch before yielding. Retain the next atomic head Load.
			// The graph also permits infinite mismatch polling: strong actor
			// fairness still advances the owner and exposes this ticket's head.
			p.PC = guardPoll
			out = append(out, guardEdge{Next: guardNormalize(n), Actor: i, Note: "mismatched head may be polled again before yield"})
		}
	}
	return out
}

func guardInvariant(s guardState) string {
	owners, claims := 0, 0
	var tickets uint8
	for _, g := range s.G {
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
	seen := map[guardState]bool{start: true}
	edges := 0
	for at := 0; at < len(nodes); at++ {
		for _, e := range guardSuccessors(nodes[at].State, c, unsafeTry) {
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
			if !seen[e.Next] {
				seen[e.Next] = true
				nodes = append(nodes, node{State: e.Next, Parent: at, Edge: e})
			}
		}
	}
	return len(nodes), edges, "", nil
}

func guardLiveness(c guardConfig, unsafeTry bool) (int, int, *fairCycle, error) {
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
			g.Enabled[at] |= 1 << uint(e.Actor)
			g.Edges[at] = append(g.Edges[at], fairEdge{To: to, Actor: e.Actor, Progress: true})
		}
	}
	f, err := findFairCycle(g)
	return len(nodes), edges, f.Cycle, err
}
