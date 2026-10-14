// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import "fmt"

// starvationWitness checks an infinite-stream property with a finite lasso.
// Each ordinary caller starts another Lock after its Unlock returns. Their
// operation counters are therefore represented modulo one, not bounded at
// one/two calls. The context caller does not restart and is never cancelled.
//
// Unlike a bounded completion search, an exact repeated state proves that
// the same fair schedule can repeat forever. All three actors take steps in
// the cycle. Real time can advance each iteration while every *fresh* waiter
// completes in <1ms; only the victim's continuous wait exceeds the threshold.
// This is stronger than merely finding a scheduler that never runs a waiter.
func starvationWitness() result {
	c := config{
		Name:   "3/unbounded-plain-stream",
		Actors: 3, Ops: 1,
		Kinds:  [4][2]kind{{contextLock}, {plainLock}, {plainLock}},
		Repeat: [4]bool{false, true, true},
	}
	s := initial(c)
	r := result{Config: c.Name, Variant: correct}
	path := []traceStep{{Actor: -1, Action: "initial", State: describe(s)}}
	seen := map[state]bool{s: true}
	var coverage uint64
	step := func(e edge) {
		if e.Error != "" {
			panic(e.Error)
		}
		s = e.Next
		if problem := invariant(s); problem != "" {
			panic(problem)
		}
		if s.G[0].Owns {
			r.VictimAcquisitions++
		}
		seen[s] = true
		coverage |= e.Cover
		path = append(path, traceStep{Step: len(path), Actor: int(e.Actor), Action: e.Note, State: describe(s)})
	}
	drive := func(who int, until phase) {
		for n := 0; s.G[who].PC != until; n++ {
			if n == 100 {
				panic(fmt.Sprintf("lasso actor %d did not reach %s: %s", who, until, describe(s)))
			}
			found := false
			for _, e := range successors(s, c, correct) {
				if int(e.Actor) == who && e.Action != cancelAction && e.Action != ageAction {
					step(e)
					found = true
					break
				}
			}
			if !found {
				panic(fmt.Sprintf("lasso actor %d cannot run toward %s: %s", who, until, describe(s)))
			}
		}
	}
	// Reach the initial wait from the zero value, rather than seeding an
	// arbitrary state. T=g0, the initial owner B=g1, barger F=g2.
	drive(1, critical)
	drive(0, parked)
	for _, e := range successors(s, c, correct) {
		if e.Actor == 0 && e.Action == ageAction {
			step(e)
			break
		}
	}
	cycle := func() {
		drive(1, entry)    // B releases; wake commits T's select.
		drive(2, critical) // F barges before T removes its registration.
		drive(1, parked)   // B starts a fresh operation and queues first.
		drive(0, parked)   // T wakes, loses, sets starvation, queues last.
		drive(2, entry)    // F hands ownership to the younger B.
		drive(1, critical) // B waited <1ms, so it turns starvation off.
	}
	cycle() // Establish T.Starved; the continuous wait has not ended.
	loop := s
	r.CycleStart = len(path) - 1
	cycle()
	if s != loop {
		panic(fmt.Sprintf("starvation schedule is not an exact lasso:\nstart %s\nend %s", describe(loop), describe(s)))
	}
	r.CycleLength = len(path) - 1 - r.CycleStart
	for _, e := range path[r.CycleStart+1:] {
		r.CycleActors[e.Actor] = true
	}
	if r.VictimAcquisitions != 0 || r.CycleActors != [4]bool{true, true, true, false} {
		panic("not a fair starvation counterexample")
	}
	r.States, r.Edges = len(seen), len(path)-1
	for i, name := range coverageNames {
		if coverage&(1<<i) != 0 {
			r.Coverage = append(r.Coverage, name)
		}
	}
	r.Failure = &failure{
		Property: "P3/fairness: old context waiter never acquires in an exact cycle where every goroutine runs",
		Trace:    path,
	}
	return r
}
