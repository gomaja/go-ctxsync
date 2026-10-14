// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Go-derived writer preference is covered by GO_LICENSE.
// Copyright 2009 The Go Authors. All rights reserved.

package ctxsync

import "context"

const readerGranted uint8 = 4

// RWMutex is a cancellable reader/writer lock. Its zero value is unlocked.
// It must not be copied after first use. Any number of readers or one writer
// may hold it. An announced writer blocks fresh readers until that writer
// acquires and releases, or cancels. Recursive reading, upgrading a read lock,
// and downgrading a write lock are unsupported, as with sync.RWMutex.
//
// Unlock synchronizes before later successful read and write acquisitions;
// RUnlock synchronizes before a subsequent successful write acquisition.
// Failed TryLock and TryRLock establish no synchronization. Writers use the
// same Mutex fairness policy, and each released reader batch reserves its
// counts before another writer may announce itself.
//
// Operations start no goroutines. Reader wait and writer drain nodes use the
// same pool as Mutex; first use, higher peak contention or an empty pool after
// GC may allocate. With sufficient warmed nodes, all paths allocate nothing
// provided context methods need no allocation, including an initialized Done
// channel.
type RWMutex struct {
	w                 Mutex
	guard             ticketGuard
	readers           int
	pending, writer   bool
	head, tail, drain *waiter
}

// Lock waits until rw is held for writing.
func (rw *RWMutex) Lock() {
	rw.w.Lock()
	_ = rw.waitWriter(context.Background())
}

// LockContext waits for write ownership or cancellation. Nil means ownership;
// ctx.Err() means no ownership. Already-done contexts never acquire. A nil
// context panics; a nil Done channel behaves like Lock. A racing cancellation
// may lose to acquisition. Cancelling an announced writer releases its readers.
func (rw *RWMutex) LockContext(ctx context.Context) error {
	if ctx == nil {
		panic("ctxsync: nil context")
	}
	if err := rw.w.LockContext(ctx); err != nil {
		return err
	}
	return rw.waitWriter(ctx)
}

// TryLock reports whether rw was acquired for writing. Failure holds nothing.
func (rw *RWMutex) TryLock() bool {
	if !rw.w.TryLock() {
		return false
	}
	if !rw.guard.TryLock() {
		rw.w.Unlock()
		return false
	}
	if rw.readers != 0 {
		rw.guard.Unlock()
		rw.w.Unlock()
		return false
	}
	rw.pending, rw.writer = true, true
	rw.guard.Unlock()
	return true
}

var testHookRWReaderWake func(*RWMutex, *waiter)
var testHookRWWriterWake func(*RWMutex)

func (rw *RWMutex) waitWriter(ctx context.Context) error {
	rw.guard.Lock()
	if err := ctx.Err(); err != nil {
		rw.guard.Unlock()
		rw.w.Unlock()
		return err
	}
	// sync/rwmutex.go, Go 1.26.9 Lock: serialize writers, announce the
	// pending writer, then drain readers active before that announcement.
	rw.pending = true
	if rw.readers == 0 {
		rw.writer = true
		rw.guard.Unlock()
		return nil
	}
	node := waiterPool.Get().(*waiter)
	rw.drain = node
	rw.guard.Unlock()
	select {
	case <-node.wake:
		if testHookRWWriterWake != nil {
			testHookRWWriterWake(rw)
		}
	case <-ctx.Done():
	}
	rw.guard.Lock()
	rw.drain = nil
	if err := ctx.Err(); err != nil {
		rw.pending = false
		rw.grantReadersLocked()
		rw.guard.Unlock()
		putWaiter(node)
		rw.w.Unlock()
		return err
	}
	rw.writer = true
	rw.guard.Unlock()
	putWaiter(node)
	return nil
}

// Unlock releases write ownership. Unlock without write ownership panics.
// Ownership is not bound to a goroutine.
func (rw *RWMutex) Unlock() {
	rw.guard.Lock()
	if !rw.writer {
		rw.guard.Unlock()
		panic("sync: Unlock of unlocked RWMutex")
	}
	rw.writer, rw.pending = false, false
	// sync/rwmutex.go, Go 1.26.9 Unlock: release queued readers before
	// allowing the next writer. Reservations include not-yet-woken readers.
	rw.grantReadersLocked()
	rw.guard.Unlock()
	rw.w.Unlock()
}

// RLock waits until rw is held for reading.
func (rw *RWMutex) RLock() { _ = rw.readLock(context.Background()) }

// RLockContext waits for read ownership or cancellation. Nil means ownership;
// ctx.Err() means no ownership or reservation. Already-done contexts never
// acquire. A nil context panics; a nil Done channel behaves like RLock.
// A racing cancellation may lose to acquisition.
func (rw *RWMutex) RLockContext(ctx context.Context) error {
	if ctx == nil {
		panic("ctxsync: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return rw.readLock(ctx)
}

// TryRLock reports whether rw was acquired for reading. It fails while a
// writer is announced or the accounting guard is busy, and holds nothing.
func (rw *RWMutex) TryRLock() bool {
	if !rw.guard.TryLock() {
		return false
	}
	if rw.pending {
		rw.guard.Unlock()
		return false
	}
	rw.readers++
	rw.guard.Unlock()
	return true
}

func (rw *RWMutex) readLock(ctx context.Context) error {
	rw.guard.Lock()
	if err := ctx.Err(); err != nil {
		rw.guard.Unlock()
		return err
	}
	if !rw.pending {
		rw.readers++
		rw.guard.Unlock()
		return nil
	}
	node := waiterPool.Get().(*waiter)
	node.mode = waiterQueued
	node.prev = rw.tail
	if rw.tail != nil {
		rw.tail.next = node
	} else {
		rw.head = node
	}
	rw.tail = node
	rw.guard.Unlock()
	select {
	case <-node.wake:
		if testHookRWReaderWake != nil {
			testHookRWReaderWake(rw, node)
		}
	case <-ctx.Done():
	}
	rw.guard.Lock()
	err := ctx.Err()
	if err != nil {
		if node.mode == readerGranted {
			rw.releaseReaderLocked()
		} else {
			rw.removeReaderLocked(node)
		}
	}
	rw.guard.Unlock()
	putWaiter(node)
	return err
}

func (rw *RWMutex) removeReaderLocked(node *waiter) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		rw.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		rw.tail = node.prev
	}
	node.prev, node.next = nil, nil
}

func (rw *RWMutex) grantReadersLocked() {
	for rw.head != nil {
		node := rw.head
		rw.removeReaderLocked(node)
		rw.readers++
		node.mode = readerGranted
		node.wake <- struct{}{}
	}
}

// RUnlock releases one read ownership. RUnlock without a read lock panics.
// A different goroutine may release an acquired read lock.
func (rw *RWMutex) RUnlock() {
	rw.guard.Lock()
	if rw.readers == 0 || rw.writer {
		rw.guard.Unlock()
		panic("sync: RUnlock of unlocked RWMutex")
	}
	rw.releaseReaderLocked()
	rw.guard.Unlock()
}

func (rw *RWMutex) releaseReaderLocked() {
	rw.readers--
	if rw.readers == 0 && rw.drain != nil {
		rw.drain.wake <- struct{}{}
	}
}
