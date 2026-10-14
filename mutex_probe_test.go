// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMutexFreshProbeAcquiresWithoutGuardAndPreservesFlags(t *testing.T) {
	var m Mutex
	m.Lock()
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	followerCtx, cancelFollower := context.WithCancel(context.Background())
	freshCtx, cancelFresh := context.WithCancel(context.Background())
	paused, resume := make(chan struct{}), make(chan struct{})
	acquired, releaseFresh := make(chan struct{}), make(chan struct{})
	results := make(chan error, 3)
	var wg sync.WaitGroup
	var wakeOnce, resumeOnce, releaseOnce, guardOnce sync.Once
	testHookMutexWake = func(mm *Mutex, node *waiter) {
		if mm == &m && node.mode == waiterWoken {
			wakeOnce.Do(func() { close(paused); <-resume })
		}
	}
	wg.Go(func() { results <- m.LockContext(firstCtx) })
	waitMutexWaiters(t, &m, 1)
	wg.Go(func() {
		err := m.LockContext(followerCtx)
		if err == nil {
			m.Unlock()
		}
		results <- err
	})
	waitMutexWaiters(t, &m, 2)
	m.Unlock()
	<-paused
	q := m.queue.Load()
	q.guard.Lock()
	t.Cleanup(func() {
		cancelFirst()
		cancelFollower()
		cancelFresh()
		guardOnce.Do(q.guard.Unlock)
		resumeOnce.Do(func() { close(resume) })
		releaseOnce.Do(func() { close(releaseFresh) })
		wg.Wait()
		testHookMutexWake = nil
	})
	wg.Go(func() {
		err := m.LockContext(freshCtx)
		if err == nil {
			close(acquired)
			<-releaseFresh
			m.Unlock()
		}
		results <- err
	})
	select {
	case <-acquired:
		if state := m.state.Load(); state != mutexLocked|mutexWoken|mutexWaiter {
			t.Fatal("fresh probe overwrote an existing wake or waiter count:", state)
		}
	case <-time.After(time.Second):
		t.Fatal("a fresh normal-mode probe could not acquire independently of the queue guard")
	}
}

func TestMutexFreshProbeLossRegisters(t *testing.T) {
	var m Mutex
	m.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := m.LockContext(ctx)
		if err == nil {
			m.Unlock()
		}
		result <- err
	}()
	t.Cleanup(func() { cancel(); m.Unlock(); <-done })
	waitMutexWaiters(t, &m, 1)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal("busy probe retained ownership or failed cancellation:", err)
	}
}

func TestMutexFreshProbeRefusesReservedHandoff(t *testing.T) {
	var m Mutex
	q := &mutexQueue{hand: &waiter{mode: waiterHanded, wake: make(chan struct{}, 1)}}
	m.queue.Store(q)
	m.state.Store(mutexStarving | mutexWaiter)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := m.LockContext(ctx)
	if err == nil {
		m.Unlock()
		t.Fatal("fresh probe acquired ownership reserved for a starvation recipient")
	}
	if !errors.Is(err, context.DeadlineExceeded) || m.state.Load() != mutexStarving|mutexWaiter {
		t.Fatal("fresh-probe cancellation changed the reserved handoff:", err, m.state.Load())
	}
}
