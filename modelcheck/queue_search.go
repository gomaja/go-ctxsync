// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"time"
)

type queueResult struct {
	Config                                string
	Variant                               variant
	States, Edges, Terminals, ReuseStates int
	Exhaustive                            bool
	Seconds                               float64
	Coverage                              []string
	Failure                               *failure `json:",omitempty"`
	CycleLength                           int      `json:",omitempty"`
}

type queueSearchNode struct {
	State        queueState
	Parent, Head uint32
	Actor        uint8
	Action       action
}

func describeQueue(s queueState) string {
	text := fmt.Sprintf("bits=%d[held=%t,woken=%t,starving=%t,waiters=%d] published=%d", s.Bits, s.Bits&heldBit != 0, s.Bits&wokenBit != 0, s.Bits&starvingBit != 0, s.Bits/waiterUnit, s.Published)
	for id, q := range s.Queues {
		if q != (waiterQueue{}) {
			text += fmt.Sprintf(" q%d={order=%v,guard=%d,hand=%d}", id, q.Order, q.Guard, q.Hand)
		}
	}
	for i, g := range s.G {
		text += fmt.Sprintf(" g%d={%s op=%d old=%d new=%d release=%d pair=%d target=%d mode=%d sendMode=%d resumeWait=%t cancel=%t started=%t aged=%t owns=%t signal=%t returnError=%t}", i, g.PC, g.Op, g.Old, g.New, g.Release, g.Pair, g.Target, g.Mode, g.SendMode, g.ResumeWait, g.Cancelled, g.Started, g.Aged, g.Owns, g.Signal, g.ReturnError)
	}
	return text
}

func queueTrace(nodes []queueSearchNode, at uint32, c config, v variant, last *queueEdge) []traceStep {
	var path []uint32
	for at != noIndex {
		path = append(path, at)
		at = nodes[at].Parent
	}
	out := []traceStep{{Actor: -1, Action: "initial", State: describeQueue(nodes[0].State)}}
	for i := len(path) - 2; i >= 0; i-- {
		n := nodes[path[i]]
		note := "transition"
		for _, e := range queueSuccessors(nodes[n.Parent].State, c, v) {
			if e.Next == n.State && e.Actor == n.Actor && e.Action == n.Action {
				note = e.Note
				break
			}
		}
		out = append(out, traceStep{Step: len(out), Actor: int(n.Actor), Action: note, State: describeQueue(n.State)})
	}
	if last != nil {
		out = append(out, traceStep{Step: len(out), Actor: int(last.Actor), Action: last.Note, State: describeQueue(last.Next)})
	}
	return out
}

func queueExplore(c config, v variant, start queueState, limit int, reuse bool) queueResult {
	began := time.Now()
	r := queueResult{Config: c.Name, Variant: v}
	nodes := []queueSearchNode{{State: queueNormalize(start), Parent: noIndex, Head: noIndex}}
	seen := map[queueState]uint32{nodes[0].State: 0}
	var links []reverseEdge
	var terminals []uint32
	var coverage uint64
	finish := func() queueResult {
		r.States = len(nodes)
		r.Seconds = time.Since(began).Seconds()
		for i, name := range queueCoverage {
			if coverage&(1<<i) != 0 {
				r.Coverage = append(r.Coverage, name)
			}
		}
		return r
	}
	fail := func(at uint32, problem string, last *queueEdge) queueResult {
		r.Failure = &failure{Property: problem, Trace: queueTrace(nodes, at, c, v, last)}
		return finish()
	}
	for at := 0; at < len(nodes); at++ {
		s := nodes[at].State
		if p := queueInvariant(s); p != "" {
			return fail(uint32(at), p, nil)
		}
		if queueTerminal(s, c) {
			if !queueIdle(s) {
				return fail(uint32(at), "P4: terminal state retains ownership, waiter, signal or guard", nil)
			}
			r.Terminals++
			terminals = append(terminals, uint32(at))
			continue
		}
		enabled := 0
		for _, e := range queueSuccessors(s, c, v) {
			r.Edges++
			coverage |= e.Cover
			if e.Error != "" {
				return fail(uint32(at), e.Error, &e)
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
				nodes = append(nodes, queueSearchNode{State: e.Next, Parent: uint32(at), Head: noIndex, Actor: e.Actor, Action: e.Action})
			}
			if e.Action != cancelAction {
				links = append(links, reverseEdge{From: uint32(at), Next: nodes[to].Head})
				nodes[to].Head = uint32(len(links) - 1)
			}
		}
		if enabled == 0 {
			return fail(uint32(at), "P3: no progress without new cancellation", nil)
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
			return fail(uint32(at), "P3: completion requires further cancellation", nil)
		}
	}
	r.Exhaustive = true
	if reuse {
		// Queue publication is the only idle residue. Every terminal is exactly
		// idle, so one probe per published identity suffices without forgetting
		// a residual flag, signal or abandoned node.
		var published [5]bool
		for _, at := range terminals {
			published[nodes[at].State.Published] = true
		}
		for id, present := range published {
			if !present {
				continue
			}
			for _, actors := range []int{1, 2} {
				probeC := config{Name: c.Name + "/reuse", Actors: actors, Ops: 1}
				probeS := queueInitial(probeC)
				probeS.Published = uint8(id)
				p := queueExplore(probeC, v, probeS, limit, false)
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
