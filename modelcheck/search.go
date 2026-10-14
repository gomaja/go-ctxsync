// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"time"
)

const noIndex = ^uint32(0)

type node struct {
	State  state
	Parent uint32
	Head   uint32
	Depth  uint16
	Actor  uint8
	Action action
}

type reverseEdge struct {
	From uint32
	Next uint32
}

type traceStep struct {
	Step   int
	Actor  int
	Action string
	State  string
}

type failure struct {
	Property string
	Trace    []traceStep
}

type result struct {
	Config             string
	Variant            variant
	States             int
	Edges              int
	Terminals          int
	Exhaustive         bool
	Seconds            float64
	Coverage           []string
	Failure            *failure `json:",omitempty"`
	Residuals          []state  `json:"-"`
	CycleStart         int      `json:",omitempty"`
	CycleLength        int      `json:",omitempty"`
	CycleActors        [4]bool  `json:",omitempty"`
	VictimAcquisitions int      `json:",omitempty"`
	ReuseStates        int      `json:",omitempty"`
	ReuseResidues      int      `json:",omitempty"`
}

func initial(c config) state {
	s := state{}
	for i := c.Actors; i < len(s.G); i++ {
		s.G[i].PC = finished
	}
	return s
}

func describe(s state) string {
	text := fmt.Sprintf("bits=%d[held=%t,woken=%t,starving=%t,waiters=%d] abandoned=%d published=%d wake=%v hand=%v",
		s.Bits, s.Bits&heldBit != 0, s.Bits&wokenBit != 0, s.Bits&starvingBit != 0,
		s.Bits/waiterUnit, s.Abandoned, s.Published, s.Wake, s.Hand)
	for i, g := range s.G {
		text += fmt.Sprintf(" g%d={%s op=%d old=%d credit=%d pair=%d queue=%d cancel=%t aged=%t starved=%t releaseStarved=%t owns=%t}",
			i, g.PC, g.Op, g.Old, g.Credit, g.Pair, g.Queue, g.Cancelled, g.Aged, g.Starved, g.ReleaseStarved, g.Owns)
	}
	return text
}

func trace(nodes []node, at uint32, c config, v variant, last *edge) []traceStep {
	var path []uint32
	for at != noIndex {
		path = append(path, at)
		at = nodes[at].Parent
	}
	out := []traceStep{{Actor: -1, Action: "initial", State: describe(nodes[0].State)}}
	for i := len(path) - 2; i >= 0; i-- {
		n := nodes[path[i]]
		prev := nodes[n.Parent].State
		note := "transition"
		for _, e := range successors(prev, c, v) {
			if e.Next == n.State && e.Actor == n.Actor && e.Action == n.Action {
				note = e.Note
				break
			}
		}
		out = append(out, traceStep{Step: len(out), Actor: int(n.Actor), Action: note, State: describe(n.State)})
	}
	if last != nil {
		out = append(out, traceStep{Step: len(out), Actor: int(last.Actor), Action: last.Note, State: describe(last.Next)})
	}
	return out
}

// explore is breadth-first: its first safety/deadlock counterexample has
// minimum transition length, including cancellation and time abstraction
// transitions. The reverse graph excludes new cancellation events. Reaching
// a terminal along it proves that additional cancellation is unnecessary.
// No state hashing approximation, random schedule, or depth cutoff is used.
func explore(c config, v variant, start state, limit int) result {
	began := time.Now()
	r := result{Config: c.Name, Variant: v}
	start = normalize(start)
	nodes := []node{{State: start, Parent: noIndex, Head: noIndex}}
	seen := map[state]uint32{start: 0}
	var links []reverseEdge
	var terminalNodes []uint32
	residuals := make(map[state]bool)
	var coverage uint64
	finish := func() result {
		r.States = len(nodes)
		r.Seconds = time.Since(began).Seconds()
		for i, name := range coverageNames {
			if coverage&(1<<i) != 0 {
				r.Coverage = append(r.Coverage, name)
			}
		}
		return r
	}
	for at := 0; at < len(nodes); at++ {
		s := nodes[at].State
		if problem := invariant(s); problem != "" {
			r.Failure = &failure{Property: problem, Trace: trace(nodes, uint32(at), c, v, nil)}
			return finish()
		}
		if terminal(s, c) {
			if c.IdleAtExit && (s.Bits != 0 || s.Abandoned != 0 || s.Hand[s.Published]) {
				r.Failure = &failure{Property: "normalization: release retained a flag, count or handoff", Trace: trace(nodes, uint32(at), c, v, nil)}
				return finish()
			}
			r.Terminals++
			terminalNodes = append(terminalNodes, uint32(at))
			s.G = [4]actor{}
			if !residuals[s] {
				residuals[s] = true
				r.Residuals = append(r.Residuals, s)
			}
			continue
		}
		enabled := 0
		for _, e := range successors(s, c, v) {
			r.Edges++
			coverage |= e.Cover
			if e.Error != "" {
				r.Failure = &failure{Property: e.Error, Trace: trace(nodes, uint32(at), c, v, &e)}
				return finish()
			}
			if e.Action != cancelAction {
				enabled++
			}
			to, exists := seen[e.Next]
			if !exists {
				if len(nodes) >= limit {
					return finish()
				}
				to = uint32(len(nodes))
				seen[e.Next] = to
				nodes = append(nodes, node{State: e.Next, Parent: uint32(at), Head: noIndex,
					Depth: nodes[at].Depth + 1, Actor: e.Actor, Action: e.Action})
			}
			if e.Action != cancelAction {
				links = append(links, reverseEdge{From: uint32(at), Next: nodes[to].Head})
				nodes[to].Head = uint32(len(links) - 1)
			}
		}
		if enabled == 0 {
			r.Failure = &failure{Property: "P3: nonterminal state has no progress without additional cancellation", Trace: trace(nodes, uint32(at), c, v, nil)}
			return finish()
		}
	}
	// Every explored state must be able to finish even if no further context
	// is cancelled. This detects closed livelock components as well as sinks.
	canFinish := make([]bool, len(nodes))
	queue := terminalNodes
	for _, at := range queue {
		canFinish[at] = true
	}
	for head := 0; head < len(queue); head++ {
		for link := nodes[queue[head]].Head; link != noIndex; link = links[link].Next {
			from := links[link].From
			if !canFinish[from] {
				canFinish[from] = true
				queue = append(queue, from)
			}
		}
	}
	for i, ok := range canFinish {
		if !ok {
			r.Failure = &failure{Property: "P3: cannot reach completion without further cancellation", Trace: trace(nodes, uint32(i), c, v, nil)}
			return finish()
		}
	}
	r.Exhaustive = true
	return finish()
}

// checkReuse probes every distinct terminal residue, first with the fresh
// Lock/Unlock required by P4, then with two concurrent fresh plain callers.
// The second probe checks that a stale wake token cannot strand a new caller.
// These are finite reuse checks, not an induction over arbitrary lifetimes.
func checkReuse(r *result, v variant, limit int) {
	for _, residue := range r.Residuals {
		r.ReuseResidues++
		for _, actors := range []int{1, 2} {
			c := config{Name: fmt.Sprintf("%s/reuse-%d", r.Config, actors), Actors: actors, Ops: 1, IdleAtExit: true}
			start := initial(c)
			start.Bits, start.Abandoned, start.Published = residue.Bits, residue.Abandoned, residue.Published
			start.Wake, start.Hand = residue.Wake, residue.Hand
			probe := explore(c, v, start, limit)
			r.ReuseStates += probe.States
			if probe.Failure != nil {
				r.Failure = probe.Failure
				r.Failure.Property = "P4 reuse: " + probe.Failure.Property
				return
			}
			if !probe.Exhaustive {
				r.Exhaustive = false
				return
			}
		}
	}
}

func configurations() []config {
	var out []config
	for _, dimensions := range [][2]int{{2, 2}, {3, 1}, {3, 2}, {4, 1}} {
		for _, mix := range []string{"plain", "context", "mixed", "try"} {
			c := config{Name: fmt.Sprintf("%dx%d/%s", dimensions[0], dimensions[1], mix), Actors: dimensions[0], Ops: dimensions[1]}
			for i := 0; i < c.Actors; i++ {
				for op := 0; op < c.Ops; op++ {
					switch mix {
					case "context":
						c.Kinds[i][op] = contextLock
					case "mixed":
						c.Kinds[i][op] = kind((i + op) % 2)
					case "try":
						c.Kinds[i][op] = kind((i + op) % 3)
					}
				}
			}
			out = append(out, c)
		}
	}
	return out
}
