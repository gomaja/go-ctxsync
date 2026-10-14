// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"testing"
)

var rwTestVariant = flag.String("rw-model-variant", string(correct), "RW model mutation")

func TestRWMutualExclusionOracle(t *testing.T) {
	s := rwInitial(rwConfigurations()[0])
	s.G[0].Owns, s.G[0].Writer = true, true
	s.G[1].Owns = true
	s.Readers = 1
	if rwInvariant(s) == "" {
		t.Fatal("a reader and writer cannot own together")
	}
}

func TestRWCancelledWriterReleasesReaders(t *testing.T) {
	c := rwConfigurations()[1]
	c.Ops = 1 // A later writer operation must not rescue a forgotten reader.
	s := rwInitial(c)
	s.Pending, s.Guard = 1, 1
	s.W.Bits = heldBit
	s.W.G[0] = queueActor{PC: qCritical, Owns: true}
	s.G[0] = rwActor{PC: rwWriteInspect, Writer: true, Cancelled: true, Started: true}
	s.G[1] = rwActor{PC: rwReadWait, Started: true}
	s.Queue = [4]uint8{2}
	v := variant(*rwTestVariant)
	for step := 0; step < 20 && !s.G[1].Signal; step++ {
		found := false
		for _, e := range rwSuccessors(s, c, v) {
			if e.Actor == 0 && e.Action != cancelAction && e.Action != ageAction {
				s = e.Next
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	if !s.G[1].Signal || !s.G[1].Granted || s.Readers != 1 || s.Pending != 0 {
		t.Fatalf("cancelled announced writer must release and reserve its queued reader: %+v", s)
	}
}

func TestRWCancelledGrantedReaderUndoesReservation(t *testing.T) {
	c := rwConfigurations()[1]
	s := rwInitial(c)
	s.Readers, s.Guard = 1, 1
	s.G[0] = rwActor{PC: rwReadInspect, Cancelled: true, Granted: true, Started: true}
	for _, e := range rwSuccessors(s, c, variant(*rwTestVariant)) {
		if e.Actor == 0 && e.Action != cancelAction && e.Action != ageAction {
			if e.Next.Readers != 0 || e.Next.G[0].Granted || e.Next.G[0].Owns {
				t.Fatalf("a cancelled granted reader must undo exactly one reservation: %+v", e.Next)
			}
			return
		}
	}
	t.Fatal("reader cleanup transition missing")
}

func TestRWCancelledUngrantReaderDoesNotSignalDrain(t *testing.T) {
	c := rwConfigurations()[1]
	s := rwInitial(c)
	s.Pending, s.Guard = 2, 1
	s.W.Bits = heldBit
	s.W.G[1] = queueActor{PC: qCritical, Owns: true}
	s.G[0] = rwActor{PC: rwReadInspect, Cancelled: true, Started: true}
	s.G[1] = rwActor{PC: rwCritical, Writer: true, Owns: true, Success: true}
	s.Queue = [4]uint8{1}
	for _, e := range rwSuccessors(s, c, correct) {
		if e.Actor == 0 && e.Action != cancelAction && e.Action != ageAction {
			if e.Next.G[0].PC != rwGuardRelease || e.Next.G[1].Drain {
				t.Fatal("removing an ungranted queued reader must not signal writer drain")
			}
			return
		}
	}
	t.Fatal("ungranted reader cleanup missing")
}

func TestRWPendingWriterBlocksFreshReaders(t *testing.T) {
	c := rwConfigurations()[0]
	s := rwInitial(c)
	s.Pending, s.Guard = 2, 1
	s.W.Bits = heldBit
	s.W.G[1] = queueActor{PC: qCritical, Owns: true}
	s.G[0] = rwActor{PC: rwReadInspect}
	s.G[1] = rwActor{PC: rwWriteWait, Writer: true, Started: true}
	for _, e := range rwSuccessors(s, c, variant(*rwTestVariant)) {
		if e.Actor == 0 && e.Action != cancelAction && e.Action != ageAction {
			if e.Next.Readers != 0 || e.Next.G[0].Owns || e.Next.Queue[0] != 1 {
				t.Fatalf("reader must queue behind an announced writer: %+v", e.Next)
			}
			return
		}
	}
	t.Fatal("reader registration transition missing")
}

func TestRWFiniteGate(t *testing.T) {
	for _, c := range rwConfigurations()[:4] {
		r := rwExplore(c, correct, 300000, true)
		if !r.Exhaustive || r.Failure != nil {
			t.Fatalf("%s: %+v", c.Name, r)
		}
	}
}

func TestRequiredRWContinuousTrafficGate(t *testing.T) {
	for _, tc := range []struct {
		name               string
		reader, cancelling bool
	}{
		{"reader/plain", true, false},
		{"reader/cancelling", true, true},
		{"writer/plain", false, false},
		{"writer/cancelling", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := rwLiveness(tc.reader, tc.cancelling, variant(*rwTestVariant), 1000000)
			if r.Failure != nil {
				t.Fatal(r.Failure.Property)
			}
			if !r.Exhaustive {
				t.Fatal("RW continuous-traffic search is inconclusive")
			}
		})
	}
}
