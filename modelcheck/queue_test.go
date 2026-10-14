// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"testing"
)

var queueTestVariant = flag.String("queue-model-variant", string(correct), "targeted-queue transition mutation")

func TestTargetedQueueFiniteGate(t *testing.T) {
	for _, c := range configurations()[:4] {
		r := queueExplore(c, correct, queueInitial(c), 500000, false)
		if r.Failure != nil || !r.Exhaustive {
			t.Fatalf("%s: %+v", c.Name, r)
		}
	}
}

func TestRequeuedWaiterRetainsPriority(t *testing.T) {
	s := queueState{}
	s.enqueue(1, 1, false)
	s.enqueue(1, 2, false)
	s.remove(1, 1)
	s.enqueue(1, 1, true)
	if s.Queues[1].Order != [4]uint8{1, 2} {
		t.Fatal("a waiter losing a wake must precede a fresh waiter")
	}
}

func TestQueueLostWakeRequeuesAtHead(t *testing.T) {
	c := config{Actors: 2, Ops: 1}
	s := queueInitial(c)
	s.Bits = heldBit + 2*waiterUnit
	s.Published = 1
	s.Queues[1] = waiterQueue{Order: [4]uint8{2}, Guard: 1}
	s.G[0] = queueActor{PC: qEnqueue, Pair: 1, Started: true, Aged: true}
	s.G[1] = queueActor{PC: qWait, Pair: 1, Started: true}
	for _, e := range queueSuccessors(s, c, variant(*queueTestVariant)) {
		if e.Actor == 0 && e.Action != ageAction {
			if e.Next.Queues[1].Order != [4]uint8{1, 2} {
				t.Fatal("a lost wake must put the old waiter ahead of the fresh one")
			}
			return
		}
	}
	t.Fatal("requeue transition missing")
}

func TestQueueHandoffPreservesOldWait(t *testing.T) {
	c := config{Actors: 2, Ops: 1}
	s := queueInitial(c)
	s.Bits = starvingBit + 2*waiterUnit
	s.Published = 1
	s.Queues[1] = waiterQueue{Order: [4]uint8{2}, Guard: 1, Hand: 1}
	s.G[0] = queueActor{PC: qInspect, Pair: 1, Mode: 2, Started: true, Aged: true}
	s.G[1] = queueActor{PC: qWait, Pair: 1, Started: true}
	for _, e := range queueSuccessors(s, c, variant(*queueTestVariant)) {
		if e.Actor == 0 && e.Action != ageAction {
			if (s.Bits+e.Next.G[0].New)&starvingBit == 0 {
				t.Fatal("an old handoff recipient with another waiter must retain starvation")
			}
			return
		}
	}
	t.Fatal("handoff transition missing")
}

func TestQueueCancelledHandoffRetainsOwnership(t *testing.T) {
	c := configurations()[1]
	r := queueExplore(c, variant(*queueTestVariant), queueInitial(c), 100000, false)
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("handoff/cancellation gate: %+v", r)
	}
}

func TestQueueCancelledNodeIsRemoved(t *testing.T) {
	c := configurations()[1]
	r := queueExplore(c, variant(*queueTestVariant), queueInitial(c), 100000, false)
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("cancelled waiters must leave an idle lock: %+v", r)
	}
}

func TestQueueNormalWakeIsNotLost(t *testing.T) {
	c := configurations()[0]
	r := queueExplore(c, variant(*queueTestVariant), queueInitial(c), 100000, false)
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("normal wake must finish finite traffic: %+v", r)
	}
}

func TestQueueLazyPublicationPreservesWaiters(t *testing.T) {
	c := configurations()[4]
	r := queueExplore(c, variant(*queueTestVariant), queueInitial(c), 100000, false)
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("all waiters must use the winning queue: %+v", r)
	}
}

func TestQueueReleaseModeIsPinned(t *testing.T) {
	c := config{Actors: 3, Ops: 1, Name: "seeded/queue-delayed-normal-release"}
	s := queueInitial(c)
	s.Bits = heldBit | starvingBit | waiterUnit
	s.Published = 1
	s.Queues[1].Order = [4]uint8{3}
	s.G[0] = queueActor{PC: qUnlockTake, Release: waiterUnit, Pair: 1}
	s.G[1] = queueActor{PC: qCritical, Owns: true, Pair: 1}
	s.G[2] = queueActor{PC: qWait, Pair: 1, Started: true, Aged: true}
	r := queueExplore(c, variant(*queueTestVariant), s, 100000, false)
	if r.Failure != nil || !r.Exhaustive {
		t.Fatalf("a prior normal release must not hand off a barger's lock: %+v", r)
	}
}

func TestQueueTryCannotBargeDuringHandoff(t *testing.T) {
	c := config{Actors: 2, Ops: 1, Kinds: [4][2]kind{{tryLock}, {plainLock}}}
	s := queueInitial(c)
	s.Bits = starvingBit + waiterUnit
	s.Published = 1
	s.Queues[1].Hand = 2
	s.G[0].PC = qTryLoad
	s.G[1] = queueActor{PC: qWait, Pair: 1, Mode: 2, Started: true, Signal: true}
	for _, e := range queueSuccessors(s, c, variant(*queueTestVariant)) {
		if e.Actor == 0 {
			if e.Next.G[0].PC != qDone {
				t.Fatal("TryLock must reject reserved starving ownership")
			}
			return
		}
	}
	t.Fatal("TryLock transition missing")
}

func TestRequiredTargetedQueueLivenessGate(t *testing.T) {
	r := queueLiveness(variant(*queueTestVariant), 100000, false)
	if r.Failure != nil {
		t.Fatal(r.Failure.Property)
	}
	if !r.Exhaustive {
		t.Fatal("queue continuous-traffic search is inconclusive")
	}
}

func TestExhaustiveSearchFindsOldChannelStarvation(t *testing.T) {
	if testing.Short() {
		t.Skip("full continuous-traffic negative control")
	}
	r := sendLockLiveness(1000000)
	if !r.Exhaustive || r.Failure == nil || r.CycleLength == 0 || !r.CycleActors[0] {
		t.Fatalf("SCC search must find the old cycle with a progressing victim: %+v", r)
	}
}

func TestQueueAcquisitionCASLossRegistersWithoutRetry(t *testing.T) {
	c := config{Actors: 2, Ops: 1}
	s := queueInitial(c)
	s.Bits, s.Published = heldBit, 1
	s.Queues[1].Guard = 1
	s.G[0] = queueActor{PC: qCAS, Pair: 1, Old: 0, New: heldBit}
	s.G[1] = queueActor{PC: qCritical, Owns: true}
	for _, e := range queueSuccessors(s, c, correct) {
		if e.Actor == 0 {
			if e.Next.G[0].PC != qRegisterAdd {
				t.Fatal("one lost acquisition CAS must register, not loop while holding the guard")
			}
			return
		}
	}
	t.Fatal("acquisition transition missing")
}

func TestQueueRegistrationWithoutOwnerProactivelyWakesSelf(t *testing.T) {
	c := config{Actors: 1, Ops: 1}
	s := queueInitial(c)
	s.Bits, s.Published = waiterUnit, 1
	s.Queues[1].Guard = 1
	s.G[0] = queueActor{PC: qEnqueue, Pair: 1, Release: waiterUnit, ResumeWait: true}
	for steps := 0; steps < 8 && !s.G[0].Signal; steps++ {
		found := false
		for _, e := range queueSuccessors(s, c, correct) {
			if e.Actor == 0 && e.Action != ageAction && e.Action != cancelAction {
				s = e.Next
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	if s.G[0].PC != qWait || !s.G[0].Signal || s.G[0].Mode != 1 || s.G[0].Owns || s.Queues[1].Guard != 0 {
		t.Fatalf("ownerless registration must wake its own pending operation: %+v", s)
	}
}

func TestQueueSendResumesPendingSender(t *testing.T) {
	c := config{Actors: 1, Ops: 1}
	s := queueInitial(c)
	s.Bits, s.Published = wokenBit, 1
	s.Queues[1].Guard = 1
	s.G[0] = queueActor{PC: qSend, Pair: 1, Target: 1, Mode: 1, SendMode: 1, ResumeWait: true, Started: true}
	for _, e := range queueSuccessors(s, c, correct) {
		if e.Actor == 0 && e.Action != ageAction {
			if e.Next.G[0].PC != qWait || !e.Next.G[0].Signal || e.Next.G[0].Mode != 1 {
				t.Fatal("self-wake sender must resume its pending wait, preserving recipient mode")
			}
			return
		}
	}
	t.Fatal("self-wake send missing")
}

func TestQueueCancelAddPreservesConcurrentUnlock(t *testing.T) {
	c := config{Actors: 2, Ops: 1, Kinds: [4][2]kind{{contextLock}, {plainLock}}}
	s := queueInitial(c)
	s.Bits, s.Published = waiterUnit, 1
	s.Queues[1] = waiterQueue{Order: [4]uint8{1}, Guard: 1}
	s.G[0] = queueActor{PC: qCancelAdd, Pair: 1, Old: heldBit + waiterUnit, New: -waiterUnit, Cancelled: true, Started: true}
	s.G[1] = queueActor{PC: qUnlockTake, Pair: 1, Release: waiterUnit}
	for _, e := range queueSuccessors(s, c, correct) {
		if e.Actor == 0 && e.Action == atomicAction {
			if e.Next.Bits != 0 || e.Next.G[0].PC != qRemove {
				t.Fatal("cancellation delta must preserve a concurrent held-bit removal without retry")
			}
			return
		}
	}
	t.Fatal("cancellation Add missing")
}

func TestQueueWakeAddPreservesConcurrentBarger(t *testing.T) {
	c := config{Actors: 3, Ops: 1}
	s := queueInitial(c)
	s.Bits, s.Published = heldBit+waiterUnit, 1
	s.Queues[1] = waiterQueue{Order: [4]uint8{2}, Guard: 1}
	s.G[0] = queueActor{PC: qWakeAdd, Pair: 1, Old: waiterUnit, New: -waiterUnit + wokenBit}
	s.G[1] = queueActor{PC: qWait, Pair: 1, Started: true}
	s.G[2] = queueActor{PC: qCritical, Owns: true}
	for _, e := range queueSuccessors(s, c, correct) {
		if e.Actor == 0 && e.Action == atomicAction {
			if e.Next.Bits != heldBit+wokenBit || e.Next.G[0].PC != qDequeue || e.Next.G[0].SendMode != 1 {
				t.Fatal("normal wake must preserve barger's held bit and advance without retry")
			}
			return
		}
	}
	t.Fatal("normal wake Add missing")
}

func TestQueueRegistrationCanEnterStarvationWhileUnlocked(t *testing.T) {
	c := config{Actors: 1, Ops: 1}
	s := queueInitial(c)
	s.Bits, s.Published = wokenBit, 1
	s.Queues[1].Guard = 1
	s.G[0] = queueActor{PC: qRegisterAdd, Pair: 1, Mode: 1, Started: true, Aged: true}
	for _, e := range queueSuccessors(s, c, correct) {
		if e.Actor == 0 && e.Action == atomicAction {
			if e.Next.Bits != starvingBit+waiterUnit || e.Next.G[0].PC != qEnqueue || !e.Next.G[0].ResumeWait {
				t.Fatal("aged registration must atomically add its waiter before reserving starvation")
			}
			return
		}
	}
	t.Fatal("registration Add missing")
}

func TestQueueSelfSenderRemainsEligibleForAgeing(t *testing.T) {
	g := queueActor{PC: qSend, Pair: 1, Target: 1, SendMode: 1, ResumeWait: true, Started: true}
	if !queueAgeEnabled(g) {
		t.Fatal("a self-waking sender is continuously waiting until it acquires")
	}
	s := queueNormalize(queueState{G: [4]queueActor{g}})
	if !s.G[0].Started {
		t.Fatal("sender phase must not reset a pending operation's wait history")
	}
}

func queueCommittedWakeFixture(c config) queueState {
	s := queueInitial(c)
	s.Bits, s.Published = heldBit+wokenBit+waiterUnit, 1
	s.Queues[1] = waiterQueue{Order: [4]uint8{3}, Guard: 2}
	s.G[0] = queueActor{PC: qUnlockAdd, Owns: true}
	s.G[1] = queueActor{PC: qInspect, Pair: 1, Mode: 1, Started: true}
	s.G[2] = queueActor{PC: qWait, Pair: 1, Started: true}
	return s
}

func queueActorStep(t *testing.T, s queueState, c config, actor uint8) queueState {
	t.Helper()
	for _, e := range queueSuccessors(s, c, variant(*queueTestVariant)) {
		if e.Actor == actor && e.Action != ageAction && e.Action != cancelAction {
			if e.Error != "" {
				t.Fatal(e.Error)
			}
			if problem := queueInvariant(e.Next); problem != "" {
				t.Fatal(problem)
			}
			return e.Next
		}
	}
	t.Fatalf("actor %d has no progress from %s", actor, describeQueue(s))
	return queueState{}
}

func TestQueueNormalWokenReleaseSkipsBusyGuard(t *testing.T) {
	c := config{Actors: 3, Ops: 1}
	s := queueCommittedWakeFixture(c)
	n := queueActorStep(t, s, c, 0)
	if n.G[0].PC != qDone || n.Bits != wokenBit+waiterUnit || n.Queues[1] != s.Queues[1] || n.G[1] != s.G[1] || n.G[2] != s.G[2] {
		t.Fatalf("normal release must leave advancement to the committed recipient without taking its busy guard: %s", describeQueue(n))
	}
}

func TestQueueStarvingReleaseDoesNotSkipGuard(t *testing.T) {
	c := config{Actors: 2, Ops: 1}
	s := queueInitial(c)
	s.Bits, s.Published = heldBit+starvingBit+waiterUnit, 1
	s.Queues[1].Order[0] = 2
	s.G[0] = queueActor{PC: qUnlockAdd, Owns: true}
	s.G[1] = queueActor{PC: qWait, Pair: 1, Started: true, Aged: true}
	n := queueActorStep(t, s, c, 0)
	if n.G[0].PC != qUnlockTake || n.G[0].Release != starvingBit+waiterUnit {
		t.Fatal("starving release must retain its pinned handoff obligation")
	}
}

func TestQueueSkippedReleaseCancelledRecipientAdvancesQueue(t *testing.T) {
	c := config{Actors: 3, Ops: 1, Kinds: [4][2]kind{{plainLock}, {contextLock}, {plainLock}}}
	s := queueCommittedWakeFixture(c)
	s.G[1].Cancelled = true
	s = queueActorStep(t, s, c, 0)
	if s.G[0].PC != qDone {
		t.Fatal("normal woken release must complete before cancellation cleanup")
	}
	for steps := 0; steps < 10 && s.G[1].PC != qDone; steps++ {
		s = queueActorStep(t, s, c, 1)
	}
	if s.G[1].PC != qDone || !s.G[2].Signal || s.G[2].Mode != 1 || s.Bits != wokenBit || s.Queues[1].Guard != 0 || s.Queues[1].Order != [4]uint8{} {
		t.Fatalf("cancelled committed recipient must make the next uncancelled waiter ready without another owner: %s", describeQueue(s))
	}
	r := queueExplore(c, variant(*queueTestVariant), s, 100000, false)
	if !r.Exhaustive || r.Failure != nil {
		t.Fatalf("the ready waiter must acquire, release, and leave an idle lock: %+v", r)
	}
}

func TestQueueSkippedReleaseLostRecipientRegistersAhead(t *testing.T) {
	c := config{Actors: 4, Ops: 1, Kinds: [4][2]kind{{plainLock}, {plainLock}, {plainLock}, {tryLock}}}
	s := queueCommittedWakeFixture(c)
	s.G[3] = queueActor{PC: qTryCAS, Old: wokenBit + waiterUnit}
	s = queueActorStep(t, s, c, 0)
	if s.G[0].PC != qDone {
		t.Fatal("release must finish while the committed recipient owns the guard")
	}
	s = queueActorStep(t, s, c, 1)
	s = queueActorStep(t, s, c, 3)
	for steps := 0; steps < 5 && s.G[1].PC != qWait; steps++ {
		s = queueActorStep(t, s, c, 1)
	}
	if s.G[1].PC != qWait || s.G[1].Mode != 0 || s.Bits != heldBit+2*waiterUnit || s.Queues[1].Order != [4]uint8{2, 3} || s.Queues[1].Guard != 0 || !s.G[3].Owns {
		t.Fatalf("losing committed recipient must register ahead of the fresh waiter and transfer advancement to the current owner: %s", describeQueue(s))
	}
	r := queueExplore(c, variant(*queueTestVariant), s, 100000, false)
	if !r.Exhaustive || r.Failure != nil {
		t.Fatalf("current owner must advance both registered waiters: %+v", r)
	}
}

func TestQueueSkippedReleaseOwnerlessRecipientAdvancesItself(t *testing.T) {
	c := config{Actors: 4, Ops: 1, Kinds: [4][2]kind{{plainLock}, {plainLock}, {plainLock}, {tryLock}}}
	s := queueCommittedWakeFixture(c)
	s.G[1].Aged = true
	s.G[3] = queueActor{PC: qTryCAS, Old: wokenBit + waiterUnit}
	s = queueActorStep(t, s, c, 0)
	s = queueActorStep(t, s, c, 1)
	s = queueActorStep(t, s, c, 3)
	s = queueActorStep(t, s, c, 1)
	if s.G[1].PC != qRegisterAdd {
		t.Fatal("the committed recipient must lose its one acquisition CAS")
	}
	s = queueActorStep(t, s, c, 3)
	s = queueActorStep(t, s, c, 3)
	if s.G[3].PC != qDone {
		t.Fatal("barger's woken release must complete before recipient registration")
	}
	for steps := 0; steps < 8 && s.G[1].PC != qWait; steps++ {
		s = queueActorStep(t, s, c, 1)
	}
	if s.G[1].PC != qWait || !s.G[1].Signal || s.G[1].Mode != 2 || s.Bits != starvingBit+2*waiterUnit || s.Queues[1].Hand != 2 || s.Queues[1].Order != [4]uint8{3} || s.Queues[1].Guard != 0 {
		t.Fatalf("ownerless old recipient must reserve starvation and hand off to itself after the skipped releases: %s", describeQueue(s))
	}
	r := queueExplore(c, variant(*queueTestVariant), s, 100000, false)
	if !r.Exhaustive || r.Failure != nil {
		t.Fatalf("self-handoff and the remaining waiter must finish: %+v", r)
	}
}

func TestQueueFreshProbePreservesNormalWakeAndCount(t *testing.T) {
	c := config{Actors: 3, Ops: 1}
	s := queueCommittedWakeFixture(c)
	s.Bits = wokenBit + waiterUnit
	s.G[0] = queueActor{PC: qProbeLoad}
	n := queueActorStep(t, s, c, 0)
	if n.G[0].PC != qProbeCAS || n.G[0].Old != s.Bits || n.G[0].New != s.Bits+heldBit || n.G[0].Pair != 0 || n.G[0].Started {
		t.Fatal("fresh normal probe must preserve its full snapshot without touching the queue or starting its wait")
	}
	n = queueActorStep(t, n, c, 0)
	if n.G[0].PC != qCritical || !n.G[0].Owns || n.Bits != s.Bits+heldBit || n.Queues != s.Queues || n.Published != s.Published || n.G[1] != s.G[1] || n.G[2] != s.G[2] {
		t.Fatal("successful fresh probe must preserve the committed wake and queued waiter while bypassing the busy guard")
	}
}

func TestQueueFreshProbeCASLossInitializesWithoutRetry(t *testing.T) {
	c := config{Actors: 2, Ops: 1}
	s := queueInitial(c)
	s.Bits = heldBit
	s.G[0] = queueActor{PC: qProbeCAS, Old: 0, New: heldBit}
	s.G[1] = queueActor{PC: qCritical, Owns: true}
	n := queueActorStep(t, s, c, 0)
	if n.G[0].PC != qInitLoad || n.Bits != s.Bits || n.G[0].Owns || n.G[0].Started || n.Published != 0 {
		t.Fatal("a failed fresh probe must immediately initialize the existing queue protocol, without a retry")
	}
}

func TestQueueFreshProbeCannotBargeDuringHandoff(t *testing.T) {
	c := config{Actors: 2, Ops: 1}
	s := queueInitial(c)
	s.Bits, s.Published = starvingBit+waiterUnit, 1
	s.Queues[1].Hand = 2
	s.G[0] = queueActor{PC: qProbeLoad}
	s.G[1] = queueActor{PC: qWait, Pair: 1, Mode: 2, Started: true, Signal: true}
	n := queueActorStep(t, s, c, 0)
	if n.G[0].PC != qInitLoad || n.Bits != s.Bits || n.G[0].Owns || n.Queues != s.Queues {
		t.Fatal("a fresh probe must reject starving ownership even when the held bit is clear")
	}
}

func TestQueueFreshProbeHeldStateInitializes(t *testing.T) {
	c := config{Actors: 2, Ops: 1}
	s := queueInitial(c)
	s.Bits = heldBit
	s.G[0] = queueActor{PC: qProbeLoad}
	s.G[1] = queueActor{PC: qCritical, Owns: true}
	n := queueActorStep(t, s, c, 0)
	if n.G[0].PC != qInitLoad || n.G[0].Owns || n.Bits != s.Bits {
		t.Fatal("a busy fresh probe must immediately continue into the existing queue protocol")
	}
}

func TestQueueFreshProbeRejectsPreviouslyCancelledContext(t *testing.T) {
	c := config{Actors: 1, Ops: 1, Kinds: [4][2]kind{{contextLock}}}
	s := queueInitial(c)
	s.G[0] = queueActor{PC: qProbeCheck, Cancelled: true}
	n := queueActorStep(t, s, c, 0)
	if n.G[0].PC != qDone || n.G[0].Owns || n.Bits != 0 || n.Published != 0 || n.Queues != [5]waiterQueue{} {
		t.Fatal("a context cancelled before the fresh probe check must return without acquisition or queue initialization")
	}
}

func TestQueueFreshProbeCancellationMayRaceAfterCheck(t *testing.T) {
	c := config{Actors: 1, Ops: 1, Kinds: [4][2]kind{{contextLock}}}
	s := queueInitial(c)
	s.G[0].PC = qProbeCheck
	s = queueActorStep(t, s, c, 0)
	if s.G[0].PC != qProbeLoad {
		t.Fatal("successful context check must precede the shared probe load")
	}
	var cancelled bool
	for _, e := range queueSuccessors(s, c, variant(*queueTestVariant)) {
		if e.Actor == 0 && e.Action == cancelAction {
			s = e.Next
			cancelled = true
			break
		}
	}
	if !cancelled {
		t.Fatal("cancellation must remain independently possible after the context check")
	}
	s = queueActorStep(t, s, c, 0)
	s = queueActorStep(t, s, c, 0)
	if s.G[0].PC != qCritical || !s.G[0].Owns || s.Bits != heldBit {
		t.Fatal("cancellation racing a successful post-check probe may lose as the acquisition contract permits")
	}
}

func TestQueueFreshProbeSnapshotLossPreservesChangedAccounting(t *testing.T) {
	c := config{Actors: 2, Ops: 1}
	s := queueInitial(c)
	s.Bits, s.Published = wokenBit, 1
	s.G[0] = queueActor{PC: qProbeCAS, Old: waiterUnit, New: waiterUnit + heldBit}
	s.G[1] = queueActor{PC: qWait, Pair: 1, Mode: 1, Started: true, Signal: true}
	n := queueActorStep(t, s, c, 0)
	if n.G[0].PC != qInitLoad || n.G[0].Owns || n.Bits != wokenBit || n.G[1] != s.G[1] {
		t.Fatal("probe CAS must compare the entire snapshot; accounting changes cannot be overwritten even when held remains clear")
	}
}
