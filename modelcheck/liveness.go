// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import "time"

func trafficConfig(name string, cancelTraffic bool) config {
	c := config{Name: name, Actors: 3, Ops: 1, Repeat: [4]bool{false, true, true}}
	c.Kinds[0][0] = contextLock
	if cancelTraffic {
		c.Kinds[1][0] = contextLock
		c.Kinds[2][0] = contextLock
	}
	return c
}

func graphAppend(g *fairGraph, active, owns, aged bool, ageMask uint16) {
	g.Edges = append(g.Edges, nil)
	g.Enabled = append(g.Enabled, 0)
	g.VictimActive = append(g.VictimActive, active)
	g.VictimOwns = append(g.VictimOwns, owns)
	g.VictimAged = append(g.VictimAged, aged)
	g.AgeMask = append(g.AgeMask, ageMask)
}

// A stopped victim is never expanded: acquiring the lock satisfies its
// obligation. This cuts irrelevant post-acquisition traffic, not any path on
// which this single pending operation is perpetually denied ownership.
func queueLiveness(v variant, limit int, cancelTraffic bool) queueResult {
	startTime := time.Now()
	c := trafficConfig("queue/fair/plain-traffic", cancelTraffic)
	if cancelTraffic {
		c.Name = "queue/fair/cancelling-traffic"
	}
	r := queueResult{Config: c.Name, Variant: v}
	start := queueInitial(c)
	nodes := []queueSearchNode{{State: start, Parent: noIndex}}
	seen := map[queueState]uint32{start: 0}
	g := fairGraph{Actors: c.Actors}
	finish := func() queueResult { r.States = len(nodes); r.Seconds = time.Since(startTime).Seconds(); return r }
	for at := 0; at < len(nodes); at++ {
		s := nodes[at].State
		victim := s.G[0]
		var ages uint16
		for i, a := range s.G {
			if queueAgeEnabled(a) {
				ages |= 1 << i
			}
		}
		// Pre-registration attempts are pending too. The aged field is an
		// acceptance eligibility marker here: no threshold is enabled before
		// Started, so those states need not satisfy an ageing obligation.
		graphAppend(&g, !victim.Cancelled, victim.Owns, victim.Aged || !victim.Started, ages)
		if victim.Owns || victim.PC == qDone {
			continue
		}
		for _, e := range queueSuccessors(s, c, v) {
			if e.Action == cancelAction && (e.Actor == 0 || !cancelTraffic) {
				continue
			}
			if e.Error != "" {
				r.Failure = &failure{Property: e.Error, Trace: queueTrace(nodes, uint32(at), c, v, &e)}
				return finish()
			}
			if problem := queueInvariant(e.Next); problem != "" {
				r.Failure = &failure{Property: problem, Trace: queueTrace(nodes, uint32(at), c, v, &e)}
				return finish()
			}
			to, ok := seen[e.Next]
			if !ok {
				if len(nodes) >= limit {
					return finish()
				}
				to = uint32(len(nodes))
				seen[e.Next] = to
				nodes = append(nodes, queueSearchNode{State: e.Next, Parent: uint32(at), Actor: e.Actor, Action: e.Action})
			}
			// Closing Done is an environmental event. Select commitment and
			// cancellation cleanup are the waiting actor's progress steps.
			progress := e.Action != ageAction && e.Action != cancelAction
			if progress {
				g.Enabled[at] |= 1 << e.Actor
			}
			g.Edges[at] = append(g.Edges[at], fairEdge{To: to, Actor: int(e.Actor), Progress: progress, Age: e.Action == ageAction})
			r.Edges++
		}
		if len(g.Edges[at]) == 0 {
			r.Failure = &failure{Property: "P3/liveness: pending queue victim at a dead end", Trace: queueTrace(nodes, uint32(at), c, v, nil)}
			return finish()
		}
	}
	f, err := findFairCycle(g)
	if err != nil {
		r.Failure = &failure{Property: "checker graph error: " + err.Error()}
		return finish()
	}
	r.Exhaustive = f.Exhaustive
	if f.Cycle != nil {
		cy := f.Cycle
		path := queueTrace(nodes, cy.Nodes[0], c, v, nil)
		for i, e := range cy.Edges {
			note := "cycle step"
			for _, candidate := range queueSuccessors(nodes[cy.Nodes[i]].State, c, v) {
				if candidate.Next == nodes[e.To].State && int(candidate.Actor) == e.Actor && (candidate.Action == ageAction) == e.Age {
					note = candidate.Note
					break
				}
			}
			path = append(path, traceStep{Step: len(path), Actor: e.Actor, Action: note, State: describeQueue(nodes[e.To].State)})
		}
		r.CycleLength = len(cy.Edges)
		r.Failure = &failure{Property: "P3/liveness: fair cycle never grants the uncancelled victim", Trace: path}
	}
	return finish()
}

func sendLockLiveness(limit int) result {
	startTime := time.Now()
	c := trafficConfig("sendlock/exhaustive-fair-traffic", false)
	r := result{Config: c.Name, Variant: correct}
	start := initial(c)
	nodes := []node{{State: start, Parent: noIndex}}
	seen := map[state]uint32{start: 0}
	g := fairGraph{Actors: c.Actors}
	finish := func() result { r.States = len(nodes); r.Seconds = time.Since(startTime).Seconds(); return r }
	for at := 0; at < len(nodes); at++ {
		s := nodes[at].State
		victim := s.G[0]
		var ages uint16
		for i, a := range s.G {
			if a.Started && !a.Aged && a.PC < cancelRecord {
				ages |= 1 << i
			}
		}
		graphAppend(&g, victim.Started && !victim.Cancelled, victim.Owns, victim.Aged, ages)
		if victim.Owns || victim.PC == finished {
			continue
		}
		for _, e := range successors(s, c, correct) {
			if e.Action == cancelAction {
				continue
			}
			if e.Error != "" {
				r.Failure = &failure{Property: e.Error, Trace: trace(nodes, uint32(at), c, correct, &e)}
				return finish()
			}
			to, ok := seen[e.Next]
			if !ok {
				if len(nodes) >= limit {
					return finish()
				}
				to = uint32(len(nodes))
				seen[e.Next] = to
				nodes = append(nodes, node{State: e.Next, Parent: uint32(at), Actor: e.Actor, Action: e.Action})
			}
			progress := e.Action != ageAction
			if progress {
				g.Enabled[at] |= 1 << e.Actor
			}
			g.Edges[at] = append(g.Edges[at], fairEdge{To: to, Actor: int(e.Actor), Progress: progress, Age: e.Action == ageAction})
			r.Edges++
		}
	}
	f, err := findFairCycle(g)
	if err != nil {
		r.Failure = &failure{Property: "checker graph error: " + err.Error()}
		return finish()
	}
	r.Exhaustive = f.Exhaustive
	if f.Cycle != nil {
		cy := f.Cycle
		path := trace(nodes, cy.Nodes[0], c, correct, nil)
		r.CycleStart = len(path) - 1
		for i, e := range cy.Edges {
			note := "cycle step"
			for _, candidate := range successors(nodes[cy.Nodes[i]].State, c, correct) {
				if candidate.Next == nodes[e.To].State && int(candidate.Actor) == e.Actor && (candidate.Action == ageAction) == e.Age {
					note = candidate.Note
					break
				}
			}
			path = append(path, traceStep{Step: len(path), Actor: e.Actor, Action: note, State: describe(nodes[e.To].State)})
			r.CycleActors[e.Actor] = true
		}
		r.CycleLength = len(cy.Edges)
		r.Failure = &failure{Property: "P3/liveness: old FIFO channel queue has a fair starvation cycle", Trace: path}
	}
	return finish()
}
