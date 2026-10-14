// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package ctxsync provides a Mutex and RWMutex with cancellable lock operations.
// Their zero values are ready for use. Neither type may be copied after first
// use. Locks are not associated with a particular goroutine.
//
// LockContext and RLockContext return nil only when the caller holds the
// requested lock. An error is ctx.Err(), and the caller holds no lock. An
// already-done context fails without acquiring. A nil context panics; a context
// whose Done channel is nil behaves like the corresponding plain lock method.
// Cancellation racing acquisition can result in either successful ownership or
// cancellation. If a cancelled waiter has received a handoff, cleanup passes or
// releases that ownership before returning an error.
//
// Mutex has one fairness policy for plain and context callers. Normal mode
// permits barging. A waiter that loses after a wake rejoins at the queue head.
// An old waiter that is still denied ownership enters starvation mode after its
// wait exceeds one millisecond. Starvation mode hands ownership to the queue
// head, and ends when the recipient is the last waiter or its own wait has not
// exceeded the threshold. The threshold selects a policy; it is not a deadline
// for acquiring or returning from a method.
//
// RWMutex permits concurrent readers or one exclusive writer. An announced
// writer blocks new readers while existing readers finish. Cancelling that
// writer releases the readers it held back. Writers share Mutex's queue policy;
// queued readers are reserved as a batch before a succeeding writer can proceed.
// Do not acquire recursive read locks, upgrade a read lock into a write lock, or
// downgrade a write lock into a read lock. Read lock holders must not modify
// shared data.
//
// Synchronization follows the successful lock boundaries of sync.Mutex and
// sync.RWMutex. A Mutex Unlock synchronizes before a later successful Lock,
// LockContext or TryLock. RWMutex writer Unlock synchronizes before subsequent
// successful read or writer acquisitions. A reader's RUnlock synchronizes
// before the next successful writer acquisition. Context methods and
// successful try methods have the corresponding plain method's guarantees.
// Failed try methods and context methods returning an error provide no such
// synchronization guarantee. See https://go.dev/ref/mem#locks.
//
// Unlock on an unlocked Mutex, or RWMutex.Unlock without a write lock, panics.
// RUnlock without an active read lock also panics. These misuse panics are
// recoverable, unlike the standard library locks' fatal errors. If a misuse
// panic is recovered, the lock's state is undefined; discard the lock. A lock
// need not be released by the goroutine that acquired it.
//
// Mutex's uncontended fast paths allocate nothing. Contention can allocate a
// lazily published queue; concurrent losing publishers discard their queues.
// Waiter nodes and their buffered wake channels are pooled. First use, increased
// peak contention, and refilling a pool emptied by garbage collection can
// allocate. Warm paths with sufficient cached nodes allocate nothing provided
// context methods need no allocation, including an initialized Done channel.
// RWMutex uses pooled reader and drain waiters as well as its writer Mutex.
// No method starts a goroutine.
package ctxsync
