// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"testing"
)

var testVariant = flag.String("model-variant", string(correct), "transition mutation for named regression tests")

func TestMutualExclusionOracle(t *testing.T) {
	s := state{}
	s.G[0].Owns = true
	s.G[1].Owns = true
	if got := invariant(s); got == "" {
		t.Fatal("two owners must violate P1")
	}
}

func TestQueuedDeliveryCommitsSelect(t *testing.T) {
	s := state{Published: 1}
	s.G[0] = actor{PC: parked, Pair: 1, Queue: 1}
	s.G[1] = actor{PC: parked, Pair: 1, Queue: 2}
	next, blocked := send(s, 1, true)
	if blocked || next.G[0].PC != handTime || next.G[1].PC != parked {
		t.Fatal("handoff must commit the oldest queued select")
	}
	if next.G[0].Queue != 0 || next.Hand[1] {
		t.Fatal("committed receive must leave neither queue entry nor token")
	}
}

func TestFullHandoffIsP2(t *testing.T) {
	s := state{}
	s.Hand[1] = true
	if _, blocked := send(s, 1, true); !blocked {
		t.Fatal("a second buffered handoff must be rejected")
	}
	if _, blocked := send(s, 1, false); blocked {
		t.Fatal("normal wake has a default case and cannot block")
	}
}

func TestCancelAfterCommittedReceiveDoesNotAbandon(t *testing.T) {
	c := config{Actors: 1, Ops: 1, Kinds: [4][2]kind{{contextLock}}}
	s := state{Published: 1, Bits: starvingBit + waiterUnit}
	s.G[0] = actor{PC: handTime, Pair: 1, Started: true}
	for _, e := range successors(s, c, correct) {
		if e.Action == cancelAction && (e.Next.G[0].PC != handTime || e.Next.Abandoned != 0) {
			t.Fatal("cancellation cannot revoke a committed handoff")
		}
	}
}

func TestRequiredStarvationGate(t *testing.T) {
	r := starvationWitness()
	if r.Failure == nil || r.CycleLength == 0 || r.CycleStart == 0 {
		t.Fatal("the sendLock-derived design has a fair infinite starvation execution; the gate must reject it")
	}
	if r.VictimAcquisitions != 0 {
		t.Fatal("the repeated cycle must never admit the old context waiter")
	}
	if r.CycleActors != [4]bool{true, true, true, false} {
		t.Fatal("every active goroutine must execute in the cycle")
	}
}

func TestHistoricalStarvationExitNormalizes(t *testing.T) {
	r := normalizationWitness(variant(*testVariant))
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("historical starvation exit must clear both flags: %+v", r.Failure)
	}
}

func TestWakeCannotBeLost(t *testing.T) {
	c := configurations()[0] // 2x2/plain
	r := explore(c, variant(*testVariant), initial(c), 100000)
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("plain callers must finish without lost wakeups: %+v", r.Failure)
	}
}

func TestLazyPublicationPreservesChannels(t *testing.T) {
	c := configurations()[4] // 3x1/plain
	r := explore(c, variant(*testVariant), initial(c), 100000)
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("all waiters must observe the winning channel pair: %+v", r.Failure)
	}
}

func TestTryRespectsStarvation(t *testing.T) {
	c := config{Name: "seeded/try-starving", Actors: 1, Ops: 1, Kinds: [4][2]kind{{tryLock}}}
	s := initial(c)
	s.Bits, s.Published, s.Hand[1] = starvingBit+waiterUnit, 1, true
	r := explore(c, variant(*testVariant), s, 1000)
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("TryLock must fail while ownership is being handed off: %+v", r.Failure)
	}
}

func TestReleaseModePinned(t *testing.T) {
	c := config{Name: "seeded/delayed-normal-unlock", Actors: 3, Ops: 1}
	s := initial(c)
	s.Bits, s.Published = heldBit|starvingBit|waiterUnit, 1
	s.G[0] = actor{PC: unlockCheck, Old: waiterUnit}
	s.G[1] = actor{PC: critical, Owns: true}
	s.G[2] = actor{PC: parked, Pair: 1, Queue: 1, Started: true, Aged: true, Starved: true}
	r := explore(c, variant(*testVariant), s, 100000)
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("delayed normal release must leave newer starvation handoff alone: %+v", r.Failure)
	}
}

func TestTerminalReuseWithStaleTokens(t *testing.T) {
	for _, residue := range []state{
		{Published: 1, Wake: [5]bool{false, true}},
		{Bits: starvingBit + waiterUnit, Abandoned: 1, Published: 1, Hand: [5]bool{false, true}},
	} {
		r := result{Config: "seeded/residue", Exhaustive: true, Residuals: []state{residue}}
		checkReuse(&r, correct, 100000)
		if r.Failure != nil || !r.Exhaustive {
			t.Fatalf("valid terminal residue must permit reuse: %+v", r.Failure)
		}
	}
}
