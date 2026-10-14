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

func TestMutexContract(t *testing.T) {
	var m Mutex
	if !m.TryLock() || m.TryLock() {
		t.Fatal("zero value and failed TryLock")
	}
	m.Unlock()
	m.Lock()
	if m.TryLock() {
		t.Fatal("TryLock acquired an owned mutex")
	}
	m.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(m.LockContext(ctx), context.Canceled) || !m.TryLock() {
		t.Fatal("cancelled entry acquired ownership")
	}
	m.Unlock()
	if err := m.LockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.Unlock()
	live, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.LockContext(live); err != nil {
		t.Fatal(err)
	}
	m.Unlock()
}

func TestMutexMisusePanics(t *testing.T) {
	var nilContext context.Context
	for _, f := range []func(){func() { var m Mutex; m.Unlock() }, func() {
		var m Mutex
		_ = m.LockContext(nilContext)
	}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("misuse did not panic")
				}
			}()
			f()
		}()
	}
}

func waitMutexWaiters(t *testing.T, m *Mutex, count int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if q := m.queue.Load(); q != nil {
			q.guard.Lock()
			n := 0
			for p := q.head; p != nil; p = p.next {
				n++
			}
			q.guard.Unlock()
			if n == count {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatal("waiter registration timed out")
}

func TestMutexCancelledWaiterLeavesNoOwnership(t *testing.T) {
	var m Mutex
	m.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- m.LockContext(ctx) }()
	waitMutexWaiters(t, &m, 1)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled waiter stranded")
	}
	m.Unlock()
	if !m.TryLock() {
		t.Fatal("cancelled waiter retained state")
	}
	m.Unlock()
}

func TestMutexMutualExclusionAndOrdering(t *testing.T) {
	var m Mutex
	value := 0
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for j := 0; j < 2000; j++ {
				m.Lock()
				value++
				m.Unlock()
			}
		})
	}
	wg.Wait()
	if value != 16000 {
		t.Fatal(value)
	}
}

func TestMutexContextStarvationFreedom(t *testing.T) {
	var m Mutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var calls atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				m.Lock()
				calls.Add(1)
				m.Unlock()
			}
		})
	}
	defer func() { close(stop); wg.Wait() }()
	for calls.Load() < 100 {
		runtime.Gosched()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.LockContext(ctx); err != nil {
		t.Fatal("context waiter starved:", err)
	}
	m.Unlock()
}

func TestMutexUncontendedAllocations(t *testing.T) {
	var m Mutex
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	value := 0
	for name, run := range map[string]func(){"plain": func() { m.Lock(); value++; m.Unlock() }, "background": func() {
		if err := m.LockContext(context.Background()); err != nil {
			panic(err)
		}
		m.Unlock()
	}, "context": func() {
		if err := m.LockContext(ctx); err != nil {
			panic(err)
		}
		m.Unlock()
	}, "try": func() {
		if !m.TryLock() {
			panic("try")
		}
		m.Unlock()
	}} {
		t.Run(name, func(t *testing.T) {
			if n := allocationTotal(t, 1000, run); n != 0 {
				t.Fatalf("uncontended path made %d total allocations", n)
			}
		})
	}
}
