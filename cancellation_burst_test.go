// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// Issue #1: when every queued caller polled the guard word, the race detector
// starved the guard's release and ticket allocation, so cancelling 128 queued
// callers with GOMAXPROCS=32 took up to 40.5 seconds in the reported runs.
// Each cancelled caller enters the guard once; together they must return
// promptly.
const cancellationBurst = 128

func raiseProcs(t *testing.T) {
	t.Helper()
	old := runtime.GOMAXPROCS(max(32, runtime.GOMAXPROCS(0)))
	t.Cleanup(func() { runtime.GOMAXPROCS(old) })
}

func awaitCancelled(t *testing.T, results <-chan error, n int) {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for i := range n {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled caller %d: %v", i, err)
			}
		case <-timeout:
			t.Fatalf("%d of %d cancelled callers returned within 3s", i, n)
		}
	}
}

func TestMutexCancellationBurstReturnsPromptly(t *testing.T) {
	raiseProcs(t)
	var m Mutex
	m.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, cancellationBurst)
	for range cancellationBurst {
		go func() {
			err := m.LockContext(ctx)
			if err == nil {
				m.Unlock()
			}
			results <- err
		}()
	}
	waitMutexWaiters(t, &m, cancellationBurst)
	cancel()
	awaitCancelled(t, results, cancellationBurst)
	if got := m.state.Load(); got != mutexLocked {
		t.Fatalf("state after cancellations = %#x, want only the owner", got)
	}
	m.Unlock()
}

func TestRWReaderCancellationBurstReturnsPromptly(t *testing.T) {
	raiseProcs(t)
	var rw RWMutex
	rw.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, cancellationBurst)
	for range cancellationBurst {
		go func() {
			err := rw.RLockContext(ctx)
			if err == nil {
				rw.RUnlock()
			}
			results <- err
		}()
	}
	waitRWPending(t, &rw, cancellationBurst)
	cancel()
	awaitCancelled(t, results, cancellationBurst)
	rw.guard.Lock()
	queued, readers := rw.head != nil, rw.readers
	rw.guard.Unlock()
	if queued || readers != 0 {
		t.Fatalf("after cancellations: readers queued=%v, reserved=%d", queued, readers)
	}
	rw.Unlock()
}
