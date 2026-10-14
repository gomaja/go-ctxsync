// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"time"
)

type rwResult struct {
	Config                                string
	Variant                               variant
	States, Edges, Terminals, ReuseStates int
	Exhaustive                            bool
	Seconds                               float64
	Coverage                              []string
	Failure                               *failure `json:",omitempty"`
	CycleLength                           int      `json:",omitempty"`
}

type rwNode struct {
	State        rwState
	Parent, Head uint32
	Actor        uint8
	Action       action
}

func describeRW(s rwState) string {
	return fmt.Sprintf("readers=%d pending=%d guard=%d queued=%v rw=%+v writerMutex={%s}", s.Readers, s.Pending, s.Guard, s.Queue, s.G, describeQueue(s.W))
}

func rwTrace(nodes []rwNode, at uint32, c rwConfig, v variant, last *rwEdge) []traceStep {
	var path []uint32
	for at != noIndex {
		path = append(path, at)
		at = nodes[at].Parent
	}
	out := []traceStep{{Actor: -1, Action: "initial", State: describeRW(nodes[0].State)}}
	for i := len(path) - 2; i >= 0; i-- {
		n := nodes[path[i]]
		note := "transition"
		for _, e := range rwSuccessors(nodes[n.Parent].State, c, v) {
			if e.Next == n.State && e.Actor == n.Actor && e.Action == n.Action {
				note = e.Note
				break
			}
		}
		out = append(out, traceStep{Step: len(out), Actor: int(n.Actor), Action: note, State: describeRW(n.State)})
	}
	if last != nil {
		out = append(out, traceStep{Step: len(out), Actor: int(last.Actor), Action: last.Note, State: describeRW(last.Next)})
	}
	return out
}

func rwExplore(c rwConfig, v variant, limit int, reuse bool) rwResult {
	began := time.Now()
	r := rwResult{Config: c.Name, Variant: v}
	start := rwInitial(c)
	nodes := []rwNode{{State: start, Parent: noIndex, Head: noIndex}}
	seen := map[rwState]uint32{start: 0}
	var links []reverseEdge
	var terminals []uint32
	var coverage uint64
	finish := func() rwResult {
		r.States = len(nodes)
		r.Seconds = time.Since(began).Seconds()
		for i, name := range rwCoverage {
			if coverage&(1<<i) != 0 {
				r.Coverage = append(r.Coverage, name)
			}
		}
		return r
	}
	fail := func(at uint32, p string, last *rwEdge) rwResult {
		r.Failure = &failure{Property: p, Trace: rwTrace(nodes, at, c, v, last)}
		return finish()
	}
	for at := 0; at < len(nodes); at++ {
		s := nodes[at].State
		if p := rwInvariant(s); p != "" {
			return fail(uint32(at), p, nil)
		}
		if rwTerminal(s, c) {
			if !rwIdle(s) {
				return fail(uint32(at), "P4: RW terminal retains count, announcement, queue or signal", nil)
			}
			r.Terminals++
			terminals = append(terminals, uint32(at))
			continue
		}
		enabled := 0
		for _, e := range rwSuccessors(s, c, v) {
			r.Edges++
			coverage |= e.Cover
			if e.Error != "" {
				return fail(uint32(at), e.Error, &e)
			}
			if p := rwInvariant(e.Next); p != "" {
				return fail(uint32(at), p, &e)
			}
			if e.Action != cancelAction {
				enabled++
			}
			to, ok := seen[e.Next]
			if !ok {
				if len(nodes) >= limit {
					return finish()
				}
				to = uint32(len(nodes))
				seen[e.Next] = to
				nodes = append(nodes, rwNode{State: e.Next, Parent: uint32(at), Head: noIndex, Actor: e.Actor, Action: e.Action})
			}
			if e.Action != cancelAction {
				links = append(links, reverseEdge{From: uint32(at), Next: nodes[to].Head})
				nodes[to].Head = uint32(len(links) - 1)
			}
		}
		if enabled == 0 {
			return fail(uint32(at), "P3: RW no progress without new cancellation", nil)
		}
	}
	canFinish := make([]bool, len(nodes))
	todo := terminals
	for _, at := range todo {
		canFinish[at] = true
	}
	for head := 0; head < len(todo); head++ {
		for link := nodes[todo[head]].Head; link != noIndex; link = links[link].Next {
			from := links[link].From
			if !canFinish[from] {
				canFinish[from] = true
				todo = append(todo, from)
			}
		}
	}
	for at, ok := range canFinish {
		if !ok {
			return fail(uint32(at), "P3: RW completion requires further cancellation", nil)
		}
	}
	r.Exhaustive = true
	if reuse {
		var published [5]bool
		for _, at := range terminals {
			published[nodes[at].State.W.Published] = true
		}
		for id, present := range published {
			if !present {
				continue
			}
			for _, writer := range []bool{false, true} {
				probe := rwConfig{Name: c.Name + "/reuse", Actors: 1, Ops: 1}
				if writer {
					probe.Kinds[0][0] = rwWriter
				}
				p := rwExplorePublished(probe, v, limit, uint8(id))
				r.ReuseStates += p.States
				if p.Failure != nil || !p.Exhaustive {
					r.Failure = p.Failure
					r.Exhaustive = p.Exhaustive
					return finish()
				}
			}
		}
	}
	return finish()
}

// Publication is the only permitted idle residue. Start the fresh read/write
// probes with that same queue identity; all other residual fields must be zero.
func rwExplorePublished(c rwConfig, v variant, limit int, published uint8) rwResult {
	// A one-actor probe has no competing writer and hence never needs lazy
	// queue publication. Keep the identity in a seeded state and enumerate
	// its short deterministic operation, including separate guard releases.
	s := rwInitial(c)
	s.W.Published = published
	r := rwResult{Config: c.Name, Variant: v}
	seen := map[rwState]bool{s: true}
	todo := []rwState{s}
	for head := 0; head < len(todo); head++ {
		s = todo[head]
		if rwTerminal(s, c) {
			if !rwIdle(s) {
				r.Failure = &failure{Property: "P4: fresh RW reuse leaves residue"}
				return r
			}
			r.Terminals++
			continue
		}
		for _, e := range rwSuccessors(s, c, v) {
			r.Edges++
			if e.Error != "" {
				r.Failure = &failure{Property: e.Error}
				return r
			}
			if p := rwInvariant(e.Next); p != "" {
				r.Failure = &failure{Property: p}
				return r
			}
			if !seen[e.Next] {
				if len(todo) >= limit {
					return r
				}
				seen[e.Next] = true
				todo = append(todo, e.Next)
			}
		}
	}
	r.States = len(todo)
	r.Exhaustive = r.Terminals != 0
	return r
}

func rwLiveness(readerVictim, cancelTraffic bool, v variant, limit int) rwResult {
	began := time.Now()
	name := "writer"
	if readerVictim {
		name = "reader"
	}
	c := rwConfig{Name: "RW/fair/" + name, Actors: 3, Ops: 1, Repeat: [4]bool{false, true, true}}
	if readerVictim {
		c.Kinds[0][0] = rwContextReader
		c.Kinds[1][0] = rwWriter
		c.Kinds[2][0] = rwWriter
	} else {
		c.Kinds[0][0] = rwContextWriter
		c.Kinds[1][0] = rwReader
		c.Kinds[2][0] = rwWriter
	}
	if cancelTraffic {
		c.Name += "/cancelling"
		c.Kinds[1][0] += 2
		c.Kinds[2][0] += 2
	}
	r := rwResult{Config: c.Name, Variant: v}
	start := rwInitial(c)
	nodes := []rwNode{{State: start, Parent: noIndex}}
	seen := map[rwState]uint32{start: 0}
	g := fairGraph{Actors: c.Actors}
	finish := func() rwResult { r.States = len(nodes); r.Seconds = time.Since(began).Seconds(); return r }
	for at := 0; at < len(nodes); at++ {
		s := nodes[at].State
		victim := s.G[0]
		var ages uint16
		for i, a := range s.G {
			if a.Started && !a.Aged && !a.Success {
				ages |= 1 << uint(i)
			}
		}
		// Include operations which are still waiting to register in the
		// writer queue; !Started bypasses only aged-cycle acceptance and does
		// not enable a threshold process or alter the actor's Aged state.
		graphAppend(&g, !victim.Cancelled, victim.Owns, victim.Aged || !victim.Started, ages)
		if victim.Owns || victim.PC == rwDone {
			continue
		}
		for _, e := range rwSuccessors(s, c, v) {
			if e.Action == cancelAction && (e.Actor == 0 || !cancelTraffic) {
				continue
			}
			if e.Error != "" {
				r.Failure = &failure{Property: e.Error, Trace: rwTrace(nodes, uint32(at), c, v, &e)}
				return finish()
			}
			if p := rwInvariant(e.Next); p != "" {
				r.Failure = &failure{Property: p, Trace: rwTrace(nodes, uint32(at), c, v, &e)}
				return finish()
			}
			to, ok := seen[e.Next]
			if !ok {
				if len(nodes) >= limit {
					return finish()
				}
				to = uint32(len(nodes))
				seen[e.Next] = to
				nodes = append(nodes, rwNode{State: e.Next, Parent: uint32(at), Actor: e.Actor, Action: e.Action})
			}
			progress := e.Action != ageAction && e.Action != cancelAction
			if progress {
				g.Enabled[at] |= 1 << e.Actor
			}
			g.Edges[at] = append(g.Edges[at], fairEdge{To: to, Actor: int(e.Actor), Progress: progress, Age: e.Action == ageAction})
			r.Edges++
		}
		if len(g.Edges[at]) == 0 {
			r.Failure = &failure{Property: "P3/liveness: pending RW victim at a dead end", Trace: rwTrace(nodes, uint32(at), c, v, nil)}
			return finish()
		}
	}
	f, err := findFairCycle(g)
	if err != nil {
		r.Failure = &failure{Property: "RW fair graph error: " + err.Error()}
		return finish()
	}
	r.Exhaustive = f.Exhaustive
	if f.Cycle != nil {
		cy := f.Cycle
		path := rwTrace(nodes, cy.Nodes[0], c, v, nil)
		for i, e := range cy.Edges {
			note := "cycle step"
			for _, candidate := range rwSuccessors(nodes[cy.Nodes[i]].State, c, v) {
				if candidate.Next == nodes[e.To].State && int(candidate.Actor) == e.Actor && (candidate.Action == ageAction) == e.Age {
					note = candidate.Note
					break
				}
			}
			path = append(path, traceStep{Step: len(path), Actor: e.Actor, Action: note, State: describeRW(nodes[e.To].State)})
		}
		r.CycleLength = len(cy.Edges)
		r.Failure = &failure{Property: "P3/liveness: fair cycle starves uncancelled RW " + name, Trace: path}
	}
	return finish()
}
