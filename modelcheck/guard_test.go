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

type guardFamilyTotals struct {
	Configurations, States, Edges int
	Exhaustive, Passing           bool
}

type guardPolicyTotals struct {
	guardFamilyTotals
	Bounded, ContinuousTraffic guardFamilyTotals
}

func TestTicketGuardBoundedAndFair(t *testing.T) {
	var totals struct {
		ProductPolicy, ConservativePolicy, CollisionPolicy guardPolicyTotals
	}
	for _, policy := range []struct {
		name       string
		policy     guardPollingPolicy
		collisions bool
		result     *guardPolicyTotals
	}{
		{"product", guardProductPolicy, false, &totals.ProductPolicy},
		{"conservative", guardConservativePolicy, false, &totals.ConservativePolicy},
		{"product-collisions", guardProductPolicy, true, &totals.CollisionPolicy},
	} {
		t.Run(policy.name, func(t *testing.T) {
			r := policy.result
			for actors := 1; actors <= 4; actors++ {
				for methods := 0; methods < 1<<uint(actors); methods++ {
					c := guardConfig{Actors: actors, Policy: policy.policy, Collisions: policy.collisions}
					for i := 0; i < actors; i++ {
						c.Try[i] = methods&(1<<uint(i)) != 0
					}
					n, edges, problem, path := guardExplore(c, false)
					if problem != "" {
						t.Fatalf("actors=%d methods=%b: %s trace=%+v", actors, methods, problem, path)
					}
					r.Bounded.Configurations++
					r.Bounded.States += n
					r.Bounded.Edges += edges
				}
			}
			for actors := 2; actors <= 4; actors++ {
				for methods := 0; methods < 1<<uint(actors-1); methods++ {
					c := guardConfig{Actors: actors, Policy: policy.policy, Collisions: policy.collisions}
					for i := 1; i < actors; i++ {
						c.Repeat[i] = true
						c.Try[i] = methods&(1<<uint(i-1)) != 0
					}
					n, edges, cycle, err := guardLiveness(c, false)
					if err != nil || cycle != nil {
						t.Fatalf("guard starvation actors=%d methods=%b states=%d cycle=%+v err=%v", actors, methods, n, cycle, err)
					}
					r.ContinuousTraffic.Configurations++
					r.ContinuousTraffic.States += n
					r.ContinuousTraffic.Edges += edges
				}
			}
			if r.Bounded.Configurations != 30 || r.ContinuousTraffic.Configurations != 14 {
				t.Fatal("guard gate must cover all 30 bounded and 14 continuous-traffic configurations")
			}
			for _, family := range []*guardFamilyTotals{&r.Bounded, &r.ContinuousTraffic} {
				family.Exhaustive, family.Passing = true, true
				r.Configurations += family.Configurations
				r.States += family.States
				r.Edges += family.Edges
			}
			r.Exhaustive, r.Passing = true, true
			if policy.policy == guardConservativePolicy && (r.States != 263419 || r.Edges != 1069253) {
				t.Fatalf("historical conservative totals changed: %+v", r)
			}
			t.Logf("bounded: %d configurations %d states %d edges", r.Bounded.Configurations, r.Bounded.States, r.Bounded.Edges)
			t.Logf("continuous traffic: %d configurations %d states %d edges", r.ContinuousTraffic.Configurations, r.ContinuousTraffic.States, r.ContinuousTraffic.Edges)
			t.Logf("ticket guard bounded and liveness total: %d states %d edges", r.States, r.Edges)
		})
	}
	if !t.Failed() {
		guardSaveEvidence(t, "CTXSYNC_GUARD_TOTALS_EVIDENCE", totals)
	}
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
	guardSaveEvidence(t, "CTXSYNC_GUARD_EVIDENCE", v)
}

func guardSaveEvidence(t *testing.T, env string, v any) {
	t.Helper()
	path := os.Getenv(env)
	if path == "" {
		return
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, append(data, '\n'), 0600); err != nil {
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
		want := s
		want.G[1].PC = e.Next.G[1].PC
		if e.Next != want {
			t.Fatal("mismatching ticket check may only change the caller's polling phase")
		}
	}
	if steps != 2 || !poll || !yield {
		t.Fatal("the guard graph must include both continued bounded polling and cooperative yield after a mismatch")
	}
}

func TestTicketGuardFarClaimStartsSlotProtocol(t *testing.T) {
	c := guardConfig{Actors: 3}
	s := guardInitial(c)
	s.Next = 3
	s.G[0] = guardActor{PC: guardBody, Claimed: true, Owns: true}
	s.G[1] = guardActor{PC: guardPoll, Ticket: 1, Claimed: true}
	s.G[2] = guardActor{PC: guardDecide, Ticket: 2, Claimed: true, Ahead: true}
	steps := 0
	for _, e := range guardSuccessors(s, c, false) {
		if e.Actor != 2 {
			continue
		}
		steps++
		if e.Next.G[2].PC != guardSlotLoad || e.Next.G[2].Owns {
			t.Fatal("a far claim must read its slot before yielding or loading the head again")
		}
	}
	if steps != 1 {
		t.Fatalf("a far claim must have exactly one slot-protocol successor, got %d", steps)
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

func TestTicketGuardHeadLoadRecordsEarlierClaimsAcrossWrap(t *testing.T) {
	for _, tc := range []struct {
		ticket, head uint8
		ready, ahead bool
	}{
		{0, 0, true, false},
		{1, 0, false, false},
		{2, 0, false, true},
		{3, 0, false, true},
		{7, 7, true, false},
		{0, 7, false, false},
		{1, 7, false, true},
		{2, 7, false, true},
	} {
		c := guardConfig{Actors: 1}
		s := guardInitial(c)
		s.Serving = tc.head
		s.G[0] = guardActor{PC: guardPoll, Ticket: tc.ticket, Claimed: true}
		es := guardSuccessors(s, c, false)
		if len(es) != 1 || es[0].Next.G[0].PC != guardDecide || es[0].Next.G[0].Ready != tc.ready || es[0].Next.G[0].Ahead != tc.ahead {
			t.Fatalf("head Load ticket=%d head=%d must remember ready=%t ahead=%t: %+v", tc.ticket, tc.head, tc.ready, tc.ahead, es)
		}
		// The head can advance after the Load. Decide must still use that
		// observation, rather than recompute rank from the current counters.
		s = es[0].Next
		s.Serving = s.G[0].Ticket
		es = guardSuccessors(s, c, false)
		want := 2
		if tc.ready || tc.ahead {
			want = 1
		}
		if len(es) != want {
			t.Fatalf("head advance changed the saved branch: ready=%t ahead=%t got %d successors", tc.ready, tc.ahead, len(es))
		}
		phase := guardYield
		if tc.ready {
			phase = guardBody
		} else if tc.ahead {
			phase = guardSlotLoad
		}
		if es[0].Next.G[0].PC != phase || es[0].Next.G[0].Owns != tc.ready {
			t.Fatal("head advance must not turn a saved mismatch into ownership")
		}
	}
}

func TestTicketGuardNormalizeClearsDeadLoadObservations(t *testing.T) {
	for pc := guardAllocate; pc <= guardRecheckFirst; pc++ {
		s := guardState{}
		s.G[0] = guardActor{PC: pc, Ready: true, Ahead: true}
		n := guardNormalize(s)
		want := pc == guardDecide
		if n.G[0].Ready != want || n.G[0].Ahead != want || guardNormalize(n) != n {
			t.Fatalf("phase %d must retain load observations only at Decide: %+v", pc, n.G[0])
		}
	}
}

type guardControlEvidence struct {
	Variant                 string
	Config                  guardConfig
	States, Edges           int
	Exhaustive              bool
	Property                string
	CycleStart, CycleLength int
	Initial                 guardState
	Trace                   []guardEdge
}

func TestTicketGuardSlotProtocolControls(t *testing.T) {
	var evidence []guardControlEvidence
	for _, tc := range []struct {
		name   string
		policy guardPollingPolicy
	}{
		{"notify-before-release-cas", guardNotifyBeforeRelease},
		{"far-without-head-recheck", guardOmitRecheck},
		{"head-recheck-before-slot-load", guardRecheckBeforeSlot},
		{"notify-newHead+2", guardNotifyWrongTicket},
		{"notify-only-tail-newHead>2", guardNotifyTooFew},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := guardConfig{Actors: 3, Policy: tc.policy}
			nodes, graph, edges := guardLivenessGraph(c, false)
			r, err := findFairCycle(graph)
			if err != nil || !r.Exhaustive || r.Cycle == nil {
				t.Fatalf("%s must admit a fair lost-notification cycle: states=%d edges=%d result=%+v err=%v", tc.name, len(nodes), edges, r, err)
			}
			fairValidateWitness(t, graph, *r.Cycle)
			for _, s := range nodes {
				if p := guardInvariant(s); p != "" {
					t.Fatalf("control must fail liveness, not safety: %s", p)
				}
			}
			for _, id := range r.Cycle.Nodes {
				s := nodes[id]
				if (s.G[0].PC != guardSlotPoll && s.G[0].PC != guardSlotYield) || s.G[0].Owns || s.G[0].Ticket != s.Serving || s.G[0].SeenSlot != s.Slots[s.G[0].Ticket] {
					t.Fatalf("victim must keep polling an unchanged slot at its own head: %+v", s)
				}
			}
			trace, start := guardCycleTrace(t, c, nodes, graph, r.Cycle)
			evidence = append(evidence, guardControlEvidence{tc.name, c, len(nodes), edges, true, "fair starvation: lost notification leaves head claim polling an unchanged slot", start, len(r.Cycle.Edges), guardInitial(c), trace})
			t.Logf("%s: states=%d edges=%d prefix=%d cycle=%d", tc.name, len(nodes), edges, start, len(r.Cycle.Edges))
		})
	}
	if len(evidence) != 5 {
		t.Fatal("all five slot-protocol controls must save a witness")
	}
	guardSaveEvidence(t, "CTXSYNC_GUARD_CONTROLS_EVIDENCE", evidence)
}

func guardCycleTrace(t *testing.T, c guardConfig, nodes []guardState, graph fairGraph, cycle *fairCycle) ([]guardEdge, int) {
	t.Helper()
	// Reconstruct a reachable prefix and the exact closed walk with the
	// same full states and transition notes as the separate-Try witness.
	parents := make([]uint32, len(nodes))
	via := make([]fairEdge, len(nodes))
	for i := range parents {
		parents[i] = noIndex
	}
	parents[0] = 0
	for from, es := range graph.Edges {
		for _, e := range es {
			if parents[e.To] == noIndex {
				parents[e.To], via[e.To] = uint32(from), e
			}
		}
	}
	step := func(from uint32, e fairEdge) guardEdge {
		for _, actual := range guardSuccessors(nodes[from], c, false) {
			if actual.Actor == e.Actor && actual.Next == nodes[e.To] {
				return actual
			}
		}
		t.Fatal("fair-cycle edge must replay as an actual guard transition")
		return guardEdge{}
	}
	var reverse, trace []guardEdge
	for at := cycle.Nodes[0]; at != 0; at = parents[at] {
		if parents[at] == noIndex || parents[at] >= at {
			t.Fatal("guard witness prefix must follow the reachable BFS tree")
		}
		reverse = append(reverse, step(parents[at], via[at]))
	}
	for i := len(reverse) - 1; i >= 0; i-- {
		trace = append(trace, reverse[i])
	}
	start := len(trace)
	for i, e := range cycle.Edges {
		trace = append(trace, step(cycle.Nodes[i], e))
	}
	cycleState := guardInitial(c)
	if start > 0 {
		cycleState = trace[start-1].Next
	}
	if trace[len(trace)-1].Next != cycleState {
		t.Fatal("starvation trace must return to the exact state at the cycle start")
	}
	return trace, start
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

func guardStepTo(t *testing.T, s guardState, c guardConfig, actor int, pc guardPhase) guardState {
	t.Helper()
	for _, e := range guardSuccessors(s, c, false) {
		if e.Actor == actor && e.Next.G[actor].PC == pc {
			return e.Next
		}
	}
	t.Fatalf("actor %d has no successor at phase %d from %+v", actor, pc, s)
	return guardState{}
}

func guardSlotFixture() (guardState, guardConfig) {
	c := guardConfig{Actors: 3}
	s := guardInitial(c)
	s.Next = 3
	s.G[0] = guardActor{PC: guardUnlockCAS, SeenNext: 3, Claimed: true, Owns: true}
	s.G[1] = guardActor{PC: guardPoll, Ticket: 1, Claimed: true}
	s.G[2] = guardActor{PC: guardSlotLoad, Ticket: 2, Claimed: true}
	return s, c
}

func TestTicketGuardSlotReadRecheckAndDelayedNotify(t *testing.T) {
	s, c := guardSlotFixture()
	s = guardStepTo(t, s, c, 2, guardRecheck)
	if s.G[2].SeenSlot != 0 {
		t.Fatal("slot read must remember the current value")
	}
	s = guardStepTo(t, s, c, 2, guardSlotPoll)
	var poll, yield bool
	for _, e := range guardSuccessors(s, c, false) {
		if e.Actor == 2 {
			poll = poll || e.Next.G[2].PC == guardSlotPoll
			yield = yield || e.Next.G[2].PC == guardSlotYield
			if e.Next.G[2].PC != guardSlotPoll && e.Next.G[2].PC != guardSlotYield {
				t.Fatal("unchanged slot must not reload the head")
			}
		}
	}
	if !poll || !yield {
		t.Fatal("unchanged slot must allow both bounded polling and Gosched")
	}
	s = guardStepTo(t, s, c, 2, guardSlotYield)
	s = guardStepTo(t, s, c, 0, guardNotify)
	if s.Next != 2 || s.G[0].Claimed || s.G[0].Owns || s.G[0].NotifyTicket != 1 || s.G[2].SeenSlot != 0 {
		t.Fatal("release CAS must advance head and clear ownership before a separate notification")
	}
	s = guardStepTo(t, s, c, 0, guardDone)
	if s.G[2].SeenSlot != 255 {
		t.Fatal("notification must change the saved comparison without changing the waiting phase")
	}
	s = guardStepTo(t, s, c, 2, guardSlotPoll)
	es := guardSuccessors(s, c, false)
	steps := 0
	for _, e := range es {
		if e.Actor == 2 {
			steps++
			if e.Next.G[2].PC != guardPoll || e.Next.G[2].SeenSlot != 0 {
				t.Fatal("changed slot must force a fresh head Load and forget its old sample")
			}
		}
	}
	if steps != 1 {
		t.Fatal("changed slot must have exactly one successor")
	}
}

func TestTicketGuardRecheckCoversReleaseBeforeAndAfterSlotRead(t *testing.T) {
	for _, notifyFirst := range []bool{false, true} {
		s, c := guardSlotFixture()
		if !notifyFirst {
			s = guardStepTo(t, s, c, 2, guardRecheck)
		}
		s = guardStepTo(t, s, c, 0, guardNotify)
		if notifyFirst {
			s = guardStepTo(t, s, c, 0, guardDone)
			s = guardStepTo(t, s, c, 2, guardRecheck)
		}
		s = guardStepTo(t, s, c, 2, guardPoll)
		if s.G[2].Owns {
			t.Fatal("head recheck returns to the polling loop without granting ownership")
		}
	}
	// The same recheck must return when both preceding owners have released.
	s, c := guardSlotFixture()
	s = guardStepTo(t, s, c, 2, guardRecheck)
	s.Serving = 2
	s = guardStepTo(t, s, c, 2, guardPoll)
	if s.G[2].Ticket != 0 {
		t.Fatal("recheck at the head must survive ticket normalization")
	}
}

func TestTicketGuardReleaseNotifyConditionUsesCASSnapshot(t *testing.T) {
	for _, remaining := range []uint8{0, 1, 2, 3} {
		c := guardConfig{Actors: 1}
		s := guardInitial(c)
		s.Next = remaining + 1
		s.G[0] = guardActor{PC: guardUnlockCAS, SeenNext: s.Next, Claimed: true, Owns: true}
		pc := guardDone
		if remaining >= 2 {
			pc = guardNotify
		}
		n := guardStepTo(t, s, c, 0, pc)
		if pc == guardNotify && n.G[0].NotifyTicket != 1 {
			t.Fatal("release must notify newHead+1 exactly when already allocated")
		}
	}
	s, c := guardSlotFixture()
	s.Next = 4 // An intervening allocation invalidates the entire packed CAS.
	n := guardStepTo(t, s, c, 0, guardUnlockLoad)
	if n.Serving != 0 || n.G[0].NotifyTicket != 0 || !n.G[0].Owns {
		t.Fatal("failed release CAS must retry without notifying or releasing ownership")
	}
}

func TestTicketGuardSlotNormalizationPreservesEqualityAndTargets(t *testing.T) {
	s := guardState{Next: 2, Serving: 7}
	s.Slots[1] = 255
	s.G[0] = guardActor{PC: guardSlotPoll, Ticket: 1, SeenSlot: 254, Claimed: true, CollisionUsed: true}
	s.G[1] = guardActor{PC: guardNotify, NotifyTicket: 1}
	n := guardNormalize(s)
	if n.G[0].Ticket != 2 || n.G[0].SeenSlot != 255 || !n.G[0].CollisionUsed || n.G[1].NotifyTicket != 2 || n.Slots != [8]uint8{} || guardNormalize(n) != n {
		t.Fatal("normalization must rotate observed slots and delayed targets together, preserving inequality")
	}
	// Native arithmetic wraps here; equality still distinguishes the sample.
	s.Slots[1]++
	n = guardNormalize(s)
	if n.G[0].SeenSlot != 254 {
		t.Fatal("slot counter wrap must preserve the two increments since the saved sample")
	}
	s.G[0].SeenSlot = 0
	n = guardNormalize(s)
	if n.G[0].SeenSlot != 0 {
		t.Fatal("normalization must preserve equal slot values")
	}
	for pc := guardAllocate; pc <= guardRecheckFirst; pc++ {
		s := guardState{}
		s.G[0] = guardActor{PC: pc, SeenSlot: 250, NotifyTicket: 3, CollisionUsed: true}
		n := guardNormalize(s)
		observed := pc == guardRecheck || pc == guardSlotPoll || pc == guardSlotYield
		if (n.G[0].SeenSlot != 0) != observed || n.G[0].CollisionUsed != observed || (n.G[0].NotifyTicket != 0) != (pc == guardNotify) {
			t.Fatalf("phase %d retains a dead sample or loses a pending notification: %+v", pc, n.G[0])
		}
	}
}

func TestTicketGuardCollisionInvalidatesSampleWithoutActorProgress(t *testing.T) {
	s, c := guardSlotFixture()
	c.Collisions = true
	s = guardStepTo(t, s, c, 2, guardRecheck)
	s = guardStepTo(t, s, c, 2, guardSlotPoll)
	found := false
	for _, e := range guardSuccessors(s, c, false) {
		if e.Actor == -1 {
			found = true
			s = e.Next
			break
		}
	}
	if !found || s.G[2].SeenSlot != 255 || !s.G[2].CollisionUsed || s.G[2].PC != guardSlotPoll {
		t.Fatal("environmental collision must invalidate the comparison without scheduling a claimant")
	}
	for _, e := range guardSuccessors(s, c, false) {
		if e.Actor == -1 {
			t.Fatal("one saved slot read admits at most one environmental collision")
		}
	}
	s = guardStepTo(t, s, c, 2, guardPoll)
	if s.G[2].CollisionUsed {
		t.Fatal("fresh head Load must forget the completed collision interval")
	}
}

func TestTicketGuardSlotInvariantRejectsBrokenProtocolStates(t *testing.T) {
	s, _ := guardSlotFixture()
	s.G[2].PC = guardSlotPoll
	for _, tc := range []struct {
		name  string
		alter func(*guardState)
	}{
		{"observer-without-claim", func(s *guardState) { s.G[2].Claimed = false }},
		{"observer-owning", func(s *guardState) { s.G[2].Owns = true }},
		{"too-many-slot-increments", func(s *guardState) { s.G[2].SeenSlot = 248 }},
		{"dead-sample", func(s *guardState) { s.G[2].PC = guardPoll; s.G[2].SeenSlot = 255 }},
		{"notify-still-owning", func(s *guardState) { s.G[0].PC = guardNotify }},
		{"dead-notify-target", func(s *guardState) { s.G[2].NotifyTicket = 1 }},
		{"dead-head-observation", func(s *guardState) { s.G[2].Ahead = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := s
			tc.alter(&n)
			if guardInvariant(n) == "" {
				t.Fatal("broken slot protocol must fail the safety invariant")
			}
		})
	}
}

func TestTicketGuardDelayedNotifyKeepsTargetAcrossLaterRelease(t *testing.T) {
	s, c := guardSlotFixture()
	c.Actors = 4
	c.Repeat[0] = true
	s.Next, s.G[0].SeenNext = 4, 4
	s.G[3] = guardActor{PC: guardPoll, Ticket: 3, Claimed: true}
	s = guardStepTo(t, s, c, 2, guardRecheck)
	s = guardStepTo(t, s, c, 2, guardSlotPoll)
	s = guardStepTo(t, s, c, 0, guardNotify)
	for _, pc := range []guardPhase{guardDecide, guardBody, guardUnlockLoad, guardUnlockCAS, guardNotify} {
		s = guardStepTo(t, s, c, 1, pc)
	}
	if s.G[0].NotifyTicket != 0 || s.G[1].NotifyTicket != 1 || s.G[2].Ticket != 0 {
		t.Fatal("later release must rotate the saved notification target without replacing it")
	}
	s = guardStepTo(t, s, c, 0, guardAllocate)
	if s.G[2].SeenSlot != 255 || s.G[0].Claimed {
		t.Fatal("delayed notification must reach its original claim before the releaser begins another call")
	}
}

func TestTicketGuardBoundedSearchRejectsLostNotificationCycle(t *testing.T) {
	c := guardConfig{Actors: 3, Policy: guardOmitRecheck}
	_, _, problem, _ := guardExplore(c, false)
	if problem != "fair guard starvation" {
		t.Fatalf("bounded search must reject a fair lost-notification cycle, got %q", problem)
	}
}
