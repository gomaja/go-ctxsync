// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestTicketGuardPollingYieldsAtBudget(t *testing.T) {
	var g ticketGuard
	g.Lock()
	var release sync.Once
	var rounds, acquisitions int
	testHookGuardYield = func(gg *ticketGuard, n int) {
		if gg == &g {
			rounds = n
			release.Do(g.Unlock)
		}
	}
	done := make(chan struct{})
	go func() { g.Lock(); acquisitions++; g.Unlock(); close(done) }()
	t.Cleanup(func() { release.Do(g.Unlock); <-done; testHookGuardYield = nil })
	select {
	case <-done:
		if rounds != guardPollBudget || acquisitions != 1 {
			t.Fatalf("yield after %d polls, %d acquisitions", rounds, acquisitions)
		}
	case <-time.After(time.Second):
		t.Fatal("head polling never yielded to release the earlier ticket")
	}
}

func TestTicketGuardExclusion(t *testing.T) {
	var g ticketGuard
	var wg sync.WaitGroup
	value := 0
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			for j := 0; j < 500; j++ {
				g.Lock()
				value++
				g.Unlock()
			}
		})
	}
	wg.Wait()
	if value != 2000 || !g.TryLock() {
		t.Fatal("guard exclusion or idle reuse")
	}
	g.Unlock()
}

func TestTicketGuardWrapKeepsTail(t *testing.T) {
	var g ticketGuard
	g.word.Store(uint64(math.MaxUint32)<<32 | uint64(math.MaxUint32))
	g.Lock()
	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() { g.Lock(); close(acquired); g.Unlock(); close(done) }()
	g.Unlock()
	<-acquired
	<-done
	if !g.TryLock() {
		t.Fatal("head wrap carried into the tail and left a phantom ticket")
	}
	g.Unlock()
}

func TestTicketGuardTryRejectsStaleFreeSnapshot(t *testing.T) {
	var g ticketGuard
	testHookGuardTry = func(gg *ticketGuard) {
		if gg == &g {
			gg.word.Store(uint64(math.MaxUint32))
		}
	}
	defer func() { testHookGuardTry = nil }()
	if g.TryLock() {
		t.Fatal("a stale free snapshot acquired a future ticket before the current head")
	}
}
