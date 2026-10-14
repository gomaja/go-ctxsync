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

func TestMutexFreshProbeCASLossRegisters(t *testing.T) {
	var m Mutex
	recipientCtx, cancelRecipient := context.WithCancel(context.Background())
	probeCtx, cancelProbe := context.WithCancel(context.Background())
	wakePaused, resumeWake := make(chan struct{}), make(chan struct{})
	probeSnapshot, resumeProbe := make(chan int32, 1), make(chan struct{})
	recipientResult, probeResult := make(chan error, 1), make(chan error, 1)
	var wakeOnce, resumeWakeOnce, resumeProbeOnce sync.Once
	var workers sync.WaitGroup
	held := false
	testHookMutexWake = func(mm *Mutex, node *waiter) {
		if mm == &m && node.mode == waiterWoken {
			wakeOnce.Do(func() { close(wakePaused); <-resumeWake })
		}
	}
	testHookMutexProbeBeforeCAS = func(mm *Mutex, old int32) {
		if mm == &m {
			probeSnapshot <- old
			<-resumeProbe
		}
	}
	t.Cleanup(func() {
		cancelProbe()
		cancelRecipient()
		resumeProbeOnce.Do(func() { close(resumeProbe) })
		resumeWakeOnce.Do(func() { close(resumeWake) })
		if held {
			m.Unlock()
		}
		workers.Wait()
		testHookMutexWake = nil
		testHookMutexProbeBeforeCAS = nil
		if state := m.state.Load(); state != 0 {
			t.Errorf("joined probe and recipient retained state: %d", state)
		}
	})
	lockContext := func(ctx context.Context, result chan<- error) {
		err := m.LockContext(ctx)
		if err == nil {
			m.Unlock()
		}
		result <- err
	}
	m.Lock()
	held = true
	workers.Go(func() { lockContext(recipientCtx, recipientResult) })
	waitMutexWaiters(t, &m, 1)
	m.Unlock()
	held = false
	select {
	case <-wakePaused:
	case <-time.After(time.Second):
		t.Fatal("normal recipient did not pause before adoption")
	}
	// The real recipient keeps Woken set, defeating the fresh caller's initial
	// zero-state CAS. Its next free snapshot is paused before the probe CAS.
	workers.Go(func() { lockContext(probeCtx, probeResult) })
	select {
	case old := <-probeSnapshot:
		if old != mutexWoken {
			t.Fatal("fresh probe did not observe the committed normal wake:", old)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh caller did not reach the free-snapshot probe CAS")
	}
	if !m.TryLock() {
		t.Fatal("normal-mode parent could not barge before the probe CAS")
	}
	held = true
	resumeProbeOnce.Do(func() { close(resumeProbe) })
	waitMutexWaiters(t, &m, 1)
	if state := m.state.Load(); state != mutexLocked|mutexWoken|mutexWaiter {
		t.Fatal("failed probe did not register behind the barger while preserving the wake:", state)
	}
	cancelProbe()
	select {
	case err := <-probeResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("failed probe did not cancel without ownership:", err)
		}
	case <-time.After(time.Second):
		t.Fatal("registered probe did not finish cancellation while the parent remained locked")
	}
	if state := m.state.Load(); state != mutexLocked|mutexWoken {
		t.Fatal("probe cancellation changed parent ownership or the original normal wake:", state)
	}
}
