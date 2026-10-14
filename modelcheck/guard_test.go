// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"testing"
)

func TestTicketGuardOriginalTryAcrossWrap(t *testing.T) {
	c := guardConfig{Actors: 2, Try: [4]bool{true, false}, Repeat: [4]bool{false, true}}
	n, edges, problem, path := guardExplore(c, true)
	if problem == "" || len(path) == 0 {
		t.Fatal("separate-counter Try must reproduce its stale-head wrap defect")
	}
	path = guardExtendOverlap(t, c, path)
	t.Logf("original Try negative control: states=%d edges=%d transitions=%d, overlapping guard owners", n, edges, len(path))
}

func TestTicketGuardPackedTryAcrossWrap(t *testing.T) {
	c := guardConfig{Actors: 2, Try: [4]bool{true, false}, Repeat: [4]bool{false, true}}
	n, edges, problem, path := guardExplore(c, false)
	if problem != "" {
		t.Fatalf("packed Try must remain safe across wrap: states=%d %s trace=%+v", n, problem, path)
	}
	t.Logf("packed Try wrap graph: states=%d edges=%d", n, edges)
}

func TestTicketGuardBoundedAndFair(t *testing.T) {
	var total, totalEdges int
	for actors := 1; actors <= 4; actors++ {
		for methods := 0; methods < 1<<uint(actors); methods++ {
			c := guardConfig{Actors: actors}
			for i := 0; i < actors; i++ {
				c.Try[i] = methods&(1<<uint(i)) != 0
			}
			n, edges, problem, path := guardExplore(c, false)
			total += n
			totalEdges += edges
			if problem != "" {
				t.Fatalf("actors=%d methods=%b: %s trace=%+v", actors, methods, problem, path)
			}
		}
	}
	for actors := 2; actors <= 4; actors++ {
		for methods := 0; methods < 1<<uint(actors-1); methods++ {
			c := guardConfig{Actors: actors}
			for i := 1; i < actors; i++ {
				c.Repeat[i] = true
				c.Try[i] = methods&(1<<uint(i-1)) != 0
			}
			n, edges, cycle, err := guardLiveness(c, false)
			total += n
			totalEdges += edges
			if err != nil || cycle != nil {
				t.Fatalf("guard starvation actors=%d methods=%b states=%d cycle=%+v err=%v", actors, methods, n, cycle, err)
			}
		}
	}
	t.Logf("ticket guard bounded and liveness total: %d states %d edges", total, totalEdges)
}

func TestTicketGuardSaveEvidence(t *testing.T) {
	if os.Getenv("CTXSYNC_GUARD_EVIDENCE") == "" {
		t.Skip("explicit evidence run")
	}
	c := guardConfig{Actors: 2, Try: [4]bool{true, false}, Repeat: [4]bool{false, true}}
	n, edges, p, path := guardExplore(c, true)
	path = guardExtendOverlap(t, c, path)
	v := struct {
		States, Edges int
		Property      string
		Trace         []guardEdge
	}{n, edges, p, path}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CTXSYNC_GUARD_EVIDENCE"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestTicketGuardWrapBoundaries(t *testing.T) {
	c := guardConfig{Actors: 1}
	s := guardInitial(c)
	s.Next, s.Serving = guardMask, guardMask
	es := guardSuccessors(s, c, false)
	if len(es) != 1 || es[0].Next.Next != 1 || es[0].Next.Serving != 0 || es[0].Next.G[0].Ticket != 0 || !es[0].Next.G[0].Claimed {
		t.Fatal("tail allocation wrapping must preserve head and allocate the last old ticket")
	}
	s = guardInitial(c)
	s.Serving = guardMask
	s.G[0] = guardActor{PC: guardUnlockCAS, Ticket: guardMask, SeenHead: guardMask, Claimed: true, Owns: true}
	es = guardSuccessors(s, c, false)
	if len(es) != 1 || es[0].Next.Next != 0 || es[0].Next.Serving != 0 || es[0].Next.G[0].PC != guardDone {
		t.Fatal("head CAS wrapping must not carry into the tail counter")
	}
	s = guardInitial(c)
	s.Next, s.Serving = guardMask, guardMask
	s.G[0] = guardActor{PC: guardTryCAS, SeenHead: guardMask, SeenNext: guardMask}
	es = guardSuccessors(s, c, false)
	if len(es) != 1 || es[0].Next.Next != 1 || es[0].Next.Serving != 0 || !es[0].Next.G[0].Owns {
		t.Fatal("packed Try wrapping must claim the current head and preserve its value")
	}
}

func TestTicketGuardMismatchMayPollOrYield(t *testing.T) {
	c := guardConfig{Actors: 2}
	s := guardInitial(c)
	s.Next = 2
	s.G[0] = guardActor{PC: guardBody, Claimed: true, Owns: true}
	s.G[1] = guardActor{PC: guardDecide, Ticket: 1, Claimed: true}
	var poll, yield bool
	steps := 0
	for _, e := range guardSuccessors(s, c, false) {
		if e.Actor != 1 {
			continue
		}
		steps++
		poll = poll || e.Next.G[1].PC == guardPoll
		yield = yield || e.Next.G[1].PC == guardYield
		if e.Next.Next != s.Next || e.Next.Serving != s.Serving || e.Next.G[1].Owns || !e.Next.G[1].Claimed || e.Next.G[1].Ticket != 1 {
			t.Fatal("mismatching ticket check may only change the caller's polling phase")
		}
	}
	if steps != 2 || !poll || !yield {
		t.Fatal("the guard graph must include both continued bounded polling and cooperative yield after a mismatch")
	}
}

func TestTicketGuardMatchingHeadEntersWithoutPolling(t *testing.T) {
	c := guardConfig{Actors: 1}
	s := guardInitial(c)
	s.Next = 1
	s.G[0] = guardActor{PC: guardDecide, Claimed: true, Ready: true}
	es := guardSuccessors(s, c, false)
	if len(es) != 1 || !es[0].Next.G[0].Owns || es[0].Next.G[0].PC != guardBody {
		t.Fatal("a matching current head must enter; optional polling must not postpone ownership")
	}
}

// The first failed invariant identifies ownership of a future ticket. Keep
// scheduling the rightful head claimant to show an actual owner overlap too.
func guardExtendOverlap(t *testing.T, c guardConfig, path []guardEdge) []guardEdge {
	t.Helper()
	s := path[len(path)-1].Next
	owners := func() int {
		n := 0
		for _, g := range s.G {
			if g.Owns {
				n++
			}
		}
		return n
	}
	for step := 0; step < 3 && owners() < 2; step++ {
		found := false
		for _, e := range guardSuccessors(s, c, true) {
			if e.Actor == 1 {
				path = append(path, e)
				s = e.Next
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	if owners() != 2 {
		t.Fatal("stale Try witness must allow the rightful head owner to enter concurrently")
	}
	return path
}
