// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Go-derived mode policy is covered by GO_LICENSE.
// Copyright 2024 The Go Authors. All rights reserved.

package ctxsync

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	mutexLocked int32 = 1 << iota
	mutexWoken
	mutexStarving
	mutexWaiter
)

const starvationThreshold = time.Millisecond

const (
	waiterQueued uint8 = iota + 1
	waiterWoken
	waiterHanded
)

type waiter struct {
	prev, next *waiter
	wake       chan struct{}
	mode       uint8
}

var waiterPool = sync.Pool{New: func() any { return &waiter{wake: make(chan struct{}, 1)} }}

func putWaiter(w *waiter) {
	select {
	case <-w.wake:
	default:
	}
	w.prev, w.next, w.mode = nil, nil, 0
	waiterPool.Put(w)
}

type mutexQueue struct {
	guard            ticketGuard
	head, tail, hand *waiter
}

func (q *mutexQueue) insert(w *waiter, head bool) {
	w.mode = waiterQueued
	if head {
		w.next = q.head
		if q.head != nil {
			q.head.prev = w
		} else {
			q.tail = w
		}
		q.head = w
		return
	}
	w.prev = q.tail
	if q.tail != nil {
		q.tail.next = w
	} else {
		q.head = w
	}
	q.tail = w
}

func (q *mutexQueue) remove(w *waiter) {
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		q.head = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		q.tail = w.prev
	}
	w.prev, w.next = nil, nil
}

// Mutex is a cancellable mutual exclusion lock. Its zero value is unlocked.
// A Mutex must not be copied after first use.
//
// Unlock synchronizes before a later successful Lock, LockContext or TryLock.
// A failed TryLock establishes no synchronization. Normal-mode callers may
// barge; a previously woken waiter retains queue priority after losing.
// After an old waiter loses, starvation mode hands ownership to the queue
// head. Plain and context callers share that queue and policy.
//
// Fast paths allocate nothing. Contention lazily allocates a queue (concurrent
// losing publishers discard theirs). Waiter nodes and buffered channels are
// pooled; first use, higher peak contention, or an empty pool after GC can
// allocate. With sufficient pooled nodes, waiting and cancellation allocate
// nothing provided context methods need no allocation (including an initialized
// Done channel). No operation starts a goroutine.
type Mutex struct {
	state atomic.Int32
	queue atomic.Pointer[mutexQueue]
}

// Lock waits until it holds m.
func (m *Mutex) Lock() {
	if m.state.CompareAndSwap(0, mutexLocked) {
		return
	}
	_ = m.lockSlow(context.Background())
}

// TryLock reports whether it acquired m. A failed call holds nothing.
func (m *Mutex) TryLock() bool {
	old := m.state.Load()
	if old&(mutexLocked|mutexStarving) != 0 {
		return false
	}
	return m.state.CompareAndSwap(old, old|mutexLocked)
}

// LockContext waits until it holds m or ctx is cancelled. It returns nil only
// while holding m; an error is ctx.Err() and leaves no ownership. An already
// cancelled context never acquires. A nil context panics; a context with a nil
// Done channel behaves like Lock. Cancellation racing acquisition may lose.
func (m *Mutex) LockContext(ctx context.Context) error {
	if ctx == nil {
		panic("ctxsync: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.state.CompareAndSwap(0, mutexLocked) {
		return nil
	}
	return m.lockSlow(ctx)
}

func (m *Mutex) initQueue() *mutexQueue {
	if q := m.queue.Load(); q != nil {
		return q
	}
	q := new(mutexQueue)
	if testHookMutexInit != nil {
		testHookMutexInit(m, q)
	}
	if m.queue.CompareAndSwap(nil, q) {
		return q
	}
	return m.queue.Load()
}

// testHookMutexWake is set before workers start by deterministic queue tests.
var testHookMutexWake func(*Mutex, *waiter)
var testHookMutexInit func(*Mutex, *mutexQueue)
var testHookMutexBeforeCAS func(*Mutex, int32)
var testHookMutexBeforeGuard func(*Mutex)
var testHookMutexBeforeRegistration func(*Mutex)
var testHookMutexProbeBeforeCAS func(*Mutex, int32)

func (m *Mutex) lockSlow(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// One fresh probe preserves the queue flags and permits normal barging
	// without the accounting guard. A busy snapshot or CAS loss registers;
	// retrying here could starve a caller before it joins the queue.
	probe := m.state.Load()
	if probe&(mutexLocked|mutexStarving) == 0 {
		if testHookMutexProbeBeforeCAS != nil {
			testHookMutexProbeBeforeCAS(m, probe)
		}
		if m.state.CompareAndSwap(probe, probe|mutexLocked) {
			return nil
		}
	}
	q := m.initQueue()
	var node *waiter
	var start time.Time
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	if testHookMutexBeforeGuard != nil {
		testHookMutexBeforeGuard(m)
	}
	for {
		q.guard.Lock()
		var err error
		if ctx != nil {
			err = ctx.Err()
		}
		old := m.state.Load()
		if node != nil && node.mode == waiterHanded {
			// Go 1.26.9 internal/sync/mutex.go, lockSlow lines 160-169.
			delta := mutexLocked - mutexWaiter
			if time.Since(start) <= starvationThreshold || old/mutexWaiter == 1 {
				delta -= mutexStarving
			}
			m.state.Add(delta)
			q.hand = nil
			node.mode = 0
			q.guard.Unlock()
			putWaiter(node)
			if err != nil {
				m.Unlock()
				return err
			}
			return nil
		}
		if err != nil {
			if node == nil {
				q.guard.Unlock()
				return err
			}
			delta := int32(0)
			if node.mode == waiterQueued {
				delta -= mutexWaiter
			}
			if node.mode == waiterWoken {
				delta -= mutexWoken
			}
			if (old+delta)/mutexWaiter == 0 && old&mutexStarving != 0 {
				delta -= mutexStarving
			}
			newState := m.state.Add(delta)
			if node.mode == waiterQueued {
				q.remove(node)
			}
			node.mode = 0
			handed := m.wakeLocked(q, newState)
			q.guard.Unlock()
			putWaiter(node)
			if handed {
				runtime.Gosched()
			}
			return err
		}
		if old&(mutexLocked|mutexStarving) == 0 {
			newState := old | mutexLocked
			if node != nil && node.mode == waiterWoken {
				newState &^= mutexWoken
			}
			if testHookMutexBeforeCAS != nil {
				testHookMutexBeforeCAS(m, old)
			}
			// One acquisition attempt: a loss always registers below. Retrying
			// under the guard could indefinitely exclude another pending caller.
			if m.state.CompareAndSwap(old, newState) {
				q.guard.Unlock()
				if node != nil {
					putWaiter(node)
				}
				return nil
			}
		}
		if node == nil {
			node = waiterPool.Get().(*waiter)
		}
		delta := mutexWaiter
		if node.mode == waiterWoken {
			delta -= mutexWoken
		}
		if !start.IsZero() && time.Since(start) > starvationThreshold && old&mutexStarving == 0 {
			delta += mutexStarving
		}
		// Count, woken and starvation fields are guard-protected. Fast paths
		// only toggle locked, so a fixed Add preserves their concurrent updates.
		// The added count makes starvation valid even if a holder just released.
		if testHookMutexBeforeRegistration != nil {
			testHookMutexBeforeRegistration(m)
		}
		newState := m.state.Add(delta)
		q.insert(node, !start.IsZero())
		if start.IsZero() {
			start = time.Now()
		}
		// If registration saw no owner, it owns the wake obligation itself.
		// Otherwise the current holder's eventual release advances the queue.
		handed := false
		if newState&mutexLocked == 0 {
			handed = m.wakeLocked(q, newState)
		}
		q.guard.Unlock()
		if handed {
			runtime.Gosched()
		}
		select {
		case <-node.wake:
			if testHookMutexWake != nil {
				testHookMutexWake(m, node)
			}
		case <-done:
		}
	}
}

// Unlock releases m. Unlocking an unlocked mutex panics. A locked mutex is
// not bound to a goroutine; another goroutine may unlock it.
func (m *Mutex) Unlock() {
	if newState := m.state.Add(-mutexLocked); newState != 0 {
		m.unlockSlow(newState)
	}
}

func (m *Mutex) unlockSlow(released int32) {
	if (released+mutexLocked)&mutexLocked == 0 {
		panic("sync: unlock of unlocked mutex")
	}
	// A normal wake is already assigned. Its recipient either acquires,
	// re-registers (with a proactive wake if ownerless), or cancels and
	// advances the queue. Decide from the release snapshot after validating
	// ownership: an invalid starvation Unlock can borrow into the woken bit.
	// See modelcheck/evidence/model-v4-review.md.
	if released&(mutexWoken|mutexStarving) == mutexWoken {
		return
	}
	q := m.queue.Load()
	q.guard.Lock()
	handed := m.wakeLocked(q, released)
	q.guard.Unlock()
	if handed {
		runtime.Gosched()
	}
}

// wakeLocked pins the mode observed by this release. A delayed normal
// release cannot hand off a newer holder's ownership (Go 1.26.9
// internal/sync/mutex.go, unlockSlow lines 206-224).
func (m *Mutex) wakeLocked(q *mutexQueue, released int32) bool {
	old := m.state.Load()
	if released&mutexStarving != 0 {
		if old&mutexStarving == 0 || old&mutexLocked != 0 || old/mutexWaiter == 0 || q.hand != nil {
			return false
		}
		node := q.head
		q.remove(node)
		node.mode = waiterHanded
		q.hand = node
		node.wake <- struct{}{}
		return true
	}
	// Retain the normal wake obligation across barging. This differs from
	// Go's held-bit wake optimization while preserving its mode/queue policy;
	// see modelcheck/evidence/queue-iteration-1.json.
	if old/mutexWaiter == 0 || old&(mutexWoken|mutexStarving) != 0 {
		return false
	}
	m.state.Add(-mutexWaiter + mutexWoken)
	node := q.head
	q.remove(node)
	node.mode = waiterWoken
	node.wake <- struct{}{}
	return false
}
