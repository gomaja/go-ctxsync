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

func TestMutexNormalReleaseSkipsBusyGuard(t *testing.T) {
	var m Mutex
	m.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	paused, resume := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	var resumeOnce, guardOnce sync.Once
	testHookMutexWake = func(mm *Mutex, _ *waiter) {
		if mm == &m {
			close(paused)
			<-resume
		}
	}
	go func() { result <- m.LockContext(ctx) }()
	waitMutexWaiters(t, &m, 1)
	m.Unlock()
	<-paused
	if !m.TryLock() {
		t.Fatal("normal barger")
	}
	q := m.queue.Load()
	q.guard.Lock()
	released := make(chan struct{})
	go func() { m.Unlock(); close(released) }()
	t.Cleanup(func() {
		cancel()
		guardOnce.Do(q.guard.Unlock)
		resumeOnce.Do(func() { close(resume) })
		<-released
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
		testHookMutexWake = nil
	})
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("normal release waited for a guard although a normal wake was assigned")
	}
}

func TestMutexIllegalUnlockOfReservedHandoffPanics(t *testing.T) {
	var m Mutex
	m.state.Store(mutexStarving | mutexWaiter)
	defer func() {
		if problem := recover(); problem != "sync: unlock of unlocked mutex" {
			t.Errorf("reserved-handoff Unlock panic: %v", problem)
		}
	}()
	m.Unlock()
}

func TestMutexCancelledWakeAdvancesFollower(t *testing.T) {
	var m Mutex
	m.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	paused, resume, follower := make(chan struct{}), make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	var wakeOnce, resumeOnce sync.Once
	var wg sync.WaitGroup
	testHookMutexWake = func(mm *Mutex, node *waiter) {
		if mm == &m && node.mode == waiterWoken {
			wakeOnce.Do(func() { close(paused); <-resume })
		}
	}
	t.Cleanup(func() {
		cancel()
		resumeOnce.Do(func() { close(resume) })
		wg.Wait()
		testHookMutexWake = nil
	})
	wg.Go(func() { result <- m.LockContext(ctx) })
	waitMutexWaiters(t, &m, 1)
	wg.Go(func() { m.Lock(); close(follower); m.Unlock() })
	waitMutexWaiters(t, &m, 2)
	m.Unlock()
	<-paused
	if !m.TryLock() {
		t.Fatal("normal barger")
	}
	m.Unlock()
	cancel()
	resumeOnce.Do(func() { close(resume) })
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-follower:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled normal recipient retained the following waiter's wake")
	}
	wg.Wait()
	if m.state.Load() != 0 {
		t.Fatal("normal wake cancellation did not settle the queue")
	}
}
