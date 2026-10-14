// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaiterReuseClearsTokenAndLinks(t *testing.T) {
	w := &waiter{wake: make(chan struct{}, 1), mode: waiterHanded}
	w.prev, w.next = &waiter{}, &waiter{}
	w.wake <- struct{}{}
	putWaiter(w)
	if len(w.wake) != 0 || w.prev != nil || w.next != nil || w.mode != 0 {
		t.Fatal("recycled waiter retained a token or protocol state")
	}
}

func TestMutexTryRefusesReservedHandoff(t *testing.T) {
	var m Mutex
	// This is the state between a starvation send and its recipient's adoption.
	m.state.Store(mutexStarving | mutexWaiter)
	if m.TryLock() {
		t.Fatal("TryLock stole ownership reserved for a starvation recipient")
	}
}

func TestMutexOwnerlessRegistrationWakes(t *testing.T) {
	var m Mutex
	m.Lock()
	allocated, publish := make(chan struct{}), make(chan struct{})
	var publishOnce, bargeOnce, releaseOnce sync.Once
	testHookMutexInit = func(mm *Mutex, _ *mutexQueue) {
		if mm == &m {
			close(allocated)
			<-publish
		}
	}
	testHookMutexBeforeCAS = func(mm *Mutex, _ int32) {
		if mm == &m {
			bargeOnce.Do(func() {
				if !m.TryLock() {
					panic("barge acquisition")
				}
			})
		}
	}
	testHookMutexBeforeRegistration = func(mm *Mutex) {
		if mm == &m {
			releaseOnce.Do(m.Unlock)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	result := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		err := m.LockContext(ctx)
		if err == nil {
			m.Unlock()
		}
		result <- err
	}()
	t.Cleanup(func() {
		cancel()
		publishOnce.Do(func() { close(publish) })
		<-finished
		testHookMutexInit, testHookMutexBeforeCAS, testHookMutexBeforeRegistration = nil, nil, nil
	})
	<-allocated
	m.Unlock()
	publishOnce.Do(func() { close(publish) })
	if err := <-result; err != nil {
		t.Fatal("ownerless registration retained its wake obligation:", err)
	}
}

func TestMutexStarvationFreedomAfterLostWake(t *testing.T) {
	for _, mode := range []string{"plain", "background", "context"} {
		t.Run(mode, func(t *testing.T) { testMutexStarvationFreedomAfterLostWake(t, mode) })
	}
}

func testMutexStarvationFreedomAfterLostWake(t *testing.T, mode string) {
	t.Helper()
	var m Mutex
	m.Lock()
	held := true
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	wakePaused := make(chan struct{})
	resume := make(chan struct{})
	release := make(chan struct{})
	acquired := make(chan string, 3)
	var resumeOnce, releaseOnce, wakeOnce sync.Once
	var wg sync.WaitGroup
	var target *waiter
	testHookMutexWake = func(mm *Mutex, node *waiter) {
		if mm == &m && node == target && node.mode == waiterWoken {
			wakeOnce.Do(func() { close(wakePaused); <-resume })
		}
	}
	t.Cleanup(func() {
		cancel()
		resumeOnce.Do(func() { close(resume) })
		if held {
			m.Unlock()
		}
		releaseOnce.Do(func() { close(release) })
		wg.Wait()
		testHookMutexWake = nil
	})
	wg.Go(func() {
		var err error
		switch mode {
		case "plain":
			m.Lock()
		case "background":
			err = m.LockContext(context.Background())
		case "context":
			err = m.LockContext(ctx)
		}
		if err == nil {
			acquired <- "old"
			<-release
			m.Unlock()
		}
	})
	waitMutexWaiters(t, &m, 1)
	q := m.queue.Load()
	q.guard.Lock()
	target = q.head
	q.guard.Unlock()
	// A real elapsed wait crosses the public one-millisecond threshold.
	time.Sleep(2 * time.Millisecond)
	m.Unlock()
	held = false
	select {
	case <-wakePaused:
	case <-ctx.Done():
		t.Fatal("normal wake missing")
	}
	m.Lock()
	held = true
	wg.Go(func() { m.Lock(); acquired <- "fresh"; <-release; m.Unlock() })
	waitMutexWaiters(t, &m, 1)
	wg.Go(func() {
		if err := m.LockContext(ctx); err == nil {
			acquired <- "fresh-context"
			<-release
			m.Unlock()
		}
	})
	waitMutexWaiters(t, &m, 2)
	resumeOnce.Do(func() { close(resume) })
	waitMutexWaiters(t, &m, 3)
	q.guard.Lock()
	atHead := q.head == target
	state := m.state.Load()
	q.guard.Unlock()
	if !atHead || state&mutexStarving == 0 {
		t.Fatalf("aged waiter lost priority: atHead=%v state=%d", atHead, state)
	}
	m.Unlock()
	held = false
	select {
	case who := <-acquired:
		if who != "old" {
			t.Fatal("the continuously waiting caller lost the starvation handoff to fresh traffic")
		}
		if m.state.Load()&mutexStarving == 0 {
			t.Fatal("an old handoff recipient with a queued follower exited starvation")
		}
	case <-ctx.Done():
		t.Fatal("old waiter starved")
	}
}

func TestMutexCancelledCommittedWakeNormalizes(t *testing.T) {
	var m Mutex
	m.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	paused, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	testHookMutexWake = func(mm *Mutex, node *waiter) {
		if mm == &m && node.mode == waiterWoken {
			close(paused)
			<-resume
		}
	}
	t.Cleanup(func() { cancel(); once.Do(func() { close(resume) }); testHookMutexWake = nil })
	go func() { result <- m.LockContext(ctx) }()
	waitMutexWaiters(t, &m, 1)
	m.Unlock()
	<-paused
	cancel()
	once.Do(func() { close(resume) })
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if m.state.Load() != 0 {
		t.Fatal("a cancelled normal wake left a live flag")
	}
	m.Lock()
	if m.TryLock() {
		t.Fatal("fresh reuse acquired an owned mutex")
	}
	m.Unlock()
}

func TestMutexCancelledCommittedHandoffReleasesOwnership(t *testing.T) {
	var m Mutex
	m.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	paused := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	testHookMutexWake = func(mm *Mutex, node *waiter) {
		if mm == &m && node.mode == waiterHanded {
			close(paused)
			<-resume
		}
	}
	t.Cleanup(func() { cancel(); once.Do(func() { close(resume) }); testHookMutexWake = nil })
	go func() { result <- m.LockContext(ctx) }()
	waitMutexWaiters(t, &m, 1)
	// Seed the reached starvation branch; all registration and wake steps
	// still execute through the actual lock and buffered node channel.
	for {
		old := m.state.Load()
		if m.state.CompareAndSwap(old, old|mutexStarving) {
			break
		}
	}
	m.Unlock()
	select {
	case <-paused:
	case <-time.After(3 * time.Second):
		t.Fatal("handoff missing")
	}
	cancel()
	once.Do(func() { close(resume) })
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled handoff stranded")
	}
	if !m.TryLock() {
		t.Fatal("cancelled recipient dropped handed ownership")
	}
	m.Unlock()
	if state := m.state.Load(); state != 0 {
		t.Fatal("cancelled final handoff retained a wake or starvation flag:", state)
	}
}

func TestMutexCancellationRemovesEveryQueuePosition(t *testing.T) {
	for position := 0; position < 3; position++ {
		t.Run(string(rune('A'+position)), func(t *testing.T) {
			var m Mutex
			m.Lock()
			var wg sync.WaitGroup
			var cancels [3]context.CancelFunc
			results := make(chan error, 3)
			for i := 0; i < 3; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				cancels[i] = cancel
				wg.Go(func() {
					err := m.LockContext(ctx)
					if err == nil {
						m.Unlock()
					}
					results <- err
				})
				waitMutexWaiters(t, &m, i+1)
			}
			cancels[position]()
			waitMutexWaiters(t, &m, 2)
			m.Unlock()
			wg.Wait()
			for _, cancel := range cancels {
				cancel()
			}
			cancelled := 0
			for i := 0; i < 3; i++ {
				if err := <-results; errors.Is(err, context.Canceled) {
					cancelled++
				}
			}
			if cancelled != 1 || !m.TryLock() {
				t.Fatal("queue cancellation lost ownership or affected another waiter")
			}
			m.Unlock()
		})
	}
}

func TestMutexMixedCancellationStress(t *testing.T) {
	var m Mutex
	var wg sync.WaitGroup
	value := 0
	for actor := 0; actor < 8; actor++ {
		wg.Go(func() {
			for i := 0; i < 500; i++ {
				if i%3 == 0 {
					if m.TryLock() {
						value++
						m.Unlock()
					}
					runtime.Gosched()
					continue
				}
				ctx, cancel := context.WithCancel(context.Background())
				if i%7 == 0 {
					cancel()
				}
				if err := m.LockContext(ctx); err == nil {
					value++
					m.Unlock()
				} else if !errors.Is(err, context.Canceled) {
					panic(err)
				}
				cancel()
			}
		})
	}
	wg.Wait()
	if value == 0 {
		t.Fatal("stress made no progress")
	}
}

func TestMutexConcurrentInitializationPublishesOneQueue(t *testing.T) {
	var m Mutex
	m.Lock()
	held := true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	first, second := make(chan struct{}), make(chan struct{})
	publishFirst, publishSecond := make(chan struct{}), make(chan struct{})
	var firstOnce, secondOnce sync.Once
	var initialized atomic.Int32
	var wg sync.WaitGroup
	testHookMutexInit = func(mm *Mutex, _ *mutexQueue) {
		if mm != &m {
			return
		}
		if initialized.Add(1) == 1 {
			close(first)
			<-publishFirst
		} else {
			close(second)
			<-publishSecond
		}
	}
	t.Cleanup(func() {
		cancel()
		firstOnce.Do(func() { close(publishFirst) })
		secondOnce.Do(func() { close(publishSecond) })
		if held {
			m.Unlock()
		}
		wg.Wait()
		testHookMutexInit = nil
	})
	call := func() {
		if err := m.LockContext(ctx); err == nil {
			m.Unlock()
		}
	}
	wg.Go(call)
	<-first
	wg.Go(call)
	<-second
	firstOnce.Do(func() { close(publishFirst) })
	waitMutexWaiters(t, &m, 1)
	secondOnce.Do(func() { close(publishSecond) })
	waitMutexWaiters(t, &m, 2)
	m.Unlock()
	held = false
	wg.Wait()
	if !m.TryLock() {
		t.Fatal("losing lazy publisher stranded a waiter")
	}
	m.Unlock()
}

func TestMutexWarmContendedAllocations(t *testing.T) {
	skipRacePoolAllocations(t)
	for _, mode := range []string{"plain", "context", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			var m Mutex
			jobs := make(chan context.Context)
			results := make(chan error)
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				for ctx := range jobs {
					var err error
					if ctx == nil {
						m.Lock()
					} else {
						err = m.LockContext(ctx)
					}
					if err == nil {
						m.Unlock()
					}
					results <- err
				}
			}()
			defer func() { close(jobs); <-stopped }()
			contexts := make([]context.Context, 100+allocationWarmup)
			cancels := make([]context.CancelFunc, len(contexts))
			if mode != "plain" {
				for i := range contexts {
					contexts[i], cancels[i] = context.WithCancel(context.Background())
					_ = contexts[i].Done()
					defer cancels[i]()
				}
			}
			iteration := 0
			allocs := allocationTotal(t, 100, func() {
				ctx := contexts[iteration]
				m.Lock()
				jobs <- ctx
				waitMutexWaiters(t, &m, 1)
				if mode == "cancel" {
					cancels[iteration]()
					if err := <-results; !errors.Is(err, context.Canceled) {
						panic("cancel result")
					}
					m.Unlock()
				} else {
					m.Unlock()
					if err := <-results; err != nil {
						panic(err)
					}
				}
				iteration++
			})
			if allocs != 0 {
				t.Fatalf("warm %s path: %d total allocations", mode, allocs)
			}
		})
	}
}

func TestMutexSlowCASCannotHoldQueueGuardForever(t *testing.T) {
	var m Mutex
	m.Lock()
	allocated, publish := make(chan struct{}), make(chan struct{})
	firstHeld := make(chan struct{})
	victimStarted := make(chan struct{})
	resumeTraffic := make(chan struct{})
	stop := make(chan struct{})
	release := make(chan struct{})
	var publishOnce, stopOnce, releaseOnce, firstOnce, resumeOnce sync.Once
	var guardEntries atomic.Int32
	var trafficOwns atomic.Bool
	var wg sync.WaitGroup
	testHookMutexInit = func(mm *Mutex, _ *mutexQueue) {
		if mm == &m {
			close(allocated)
			<-publish
		}
	}
	testHookMutexBeforeCAS = func(mm *Mutex, old int32) {
		if mm != &m {
			return
		}
		select {
		case <-stop:
			return
		default:
		}
		if old&mutexLocked == 0 {
			if !m.TryLock() {
				panic("traffic acquire")
			}
			trafficOwns.Store(true)
			firstOnce.Do(func() { close(firstHeld); <-resumeTraffic })
		} else {
			m.Unlock()
			trafficOwns.Store(false)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	testHookMutexBeforeGuard = func(mm *Mutex) {
		if mm == &m && guardEntries.Add(1) == 2 {
			close(victimStarted)
		}
	}
	result := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		stopOnce.Do(func() { close(stop) })
		resumeOnce.Do(func() { close(resumeTraffic) })
		publishOnce.Do(func() { close(publish) })
		releaseOnce.Do(func() { close(release) })
		if trafficOwns.Load() {
			m.Unlock()
		}
		wg.Wait()
		testHookMutexInit = nil
		testHookMutexBeforeCAS = nil
		testHookMutexBeforeGuard = nil
	})
	wg.Go(func() { m.Lock(); <-release; m.Unlock() })
	<-allocated
	m.Unlock()
	publishOnce.Do(func() { close(publish) })
	<-firstHeld
	wg.Go(func() {
		err := m.LockContext(ctx)
		if err == nil {
			m.Unlock()
		}
		result <- err
	})
	// Force a live attempt before cancellation; the held bit makes its fast
	// path fail. The competing callback keeps defeating the older CAS loop.
	<-victimStarted
	cancel()
	resumeOnce.Do(func() { close(resumeTraffic) })
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("retrying CAS held the queue guard and stranded cancellation")
	}
}
