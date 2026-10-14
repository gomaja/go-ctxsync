// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMutexDelayedNormalReleaseCannotHandOffBargersOwnership(t *testing.T) {
	var m Mutex
	targetCtx, cancelTarget := context.WithCancel(context.Background())
	freshCtx, cancelFresh := context.WithCancel(context.Background())
	wakePaused, resumeWake := make(chan struct{}), make(chan struct{})
	targetAcquired, releaseTarget := make(chan struct{}), make(chan struct{})
	freshAcquired := make(chan struct{})
	var target *waiter
	var pauseOnce, resumeOnce, releaseOnce sync.Once
	var workers sync.WaitGroup
	held := false
	testHookMutexWake = func(mm *Mutex, node *waiter) {
		if mm == &m && node == target && node.mode == waiterWoken {
			pauseOnce.Do(func() { close(wakePaused); <-resumeWake })
		}
	}
	t.Cleanup(func() {
		cancelTarget()
		cancelFresh()
		resumeOnce.Do(func() { close(resumeWake) })
		releaseOnce.Do(func() { close(releaseTarget) })
		if held {
			m.Unlock()
		}
		workers.Wait()
		testHookMutexWake = nil
		if state := m.state.Load(); state != 0 {
			t.Errorf("joined delayed-release callers retained state: %d", state)
		}
	})
	m.Lock()
	held = true
	workers.Go(func() {
		if err := m.LockContext(targetCtx); err == nil {
			close(targetAcquired)
			<-releaseTarget
			m.Unlock()
		}
	})
	waitMutexWaiters(t, &m, 1)
	q := m.queue.Load()
	q.guard.Lock()
	target = q.head
	q.guard.Unlock()
	// The target's real first-enqueue clock must be old when its lost normal
	// wake returns it to the head and activates starvation mode.
	time.Sleep(2 * starvationThreshold)
	// Split the actual Unlock Add and slow steps. This normal snapshot belongs
	// to the first owner, and its slow step must not release a later barger.
	released := m.state.Add(-mutexLocked)
	held = false
	if released != mutexWaiter || !m.TryLock() {
		t.Fatal("first release did not leave a queued waiter available for normal barging:", released)
	}
	held = true
	m.Unlock()
	held = false
	select {
	case <-wakePaused:
	case <-time.After(3 * time.Second):
		t.Fatal("target did not pause after the real normal wake")
	}
	if !m.TryLock() {
		t.Fatal("second barger could not acquire ahead of the paused normal recipient")
	}
	held = true
	workers.Go(func() {
		if err := m.LockContext(freshCtx); err == nil {
			close(freshAcquired)
			m.Unlock()
		}
	})
	waitMutexWaiters(t, &m, 1)
	resumeOnce.Do(func() { close(resumeWake) })
	waitMutexWaiters(t, &m, 2)
	q.guard.Lock()
	before := m.state.Load()
	queuedAtHead := q.head == target && target.mode == waiterQueued && q.hand == nil
	q.guard.Unlock()
	if !queuedAtHead || before != mutexLocked|mutexStarving|2*mutexWaiter {
		t.Fatal("aged target did not requeue at the head in starvation behind the current owner:", before)
	}
	// A snapshot-only reread can be masked by the additional held check. The
	// historical-transition negative must both reread the newer starvation
	// mode and omit that check, matching the archived model control.
	m.unlockSlow(released)
	q.guard.Lock()
	after := m.state.Load()
	stillQueued := q.head == target && target.mode == waiterQueued && q.hand == nil
	q.guard.Unlock()
	if after != before || !stillQueued {
		t.Fatal("delayed normal release changed the barger's ownership or handed it away:", before, after)
	}
	select {
	case <-targetAcquired:
		t.Fatal("target acquired before the current barger released")
	case <-freshAcquired:
		t.Fatal("fresh waiter acquired before the current barger released")
	default:
	}
	m.Unlock()
	held = false
	select {
	case <-targetAcquired:
	case <-time.After(3 * time.Second):
		t.Fatal("current owner's release did not hand ownership to the old head")
	}
	select {
	case <-freshAcquired:
		t.Fatal("fresh waiter overtook the handed old target")
	default:
	}
}
