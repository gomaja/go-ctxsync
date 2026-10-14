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

func TestRWMutexContract(t *testing.T) {
	var rw RWMutex
	if !rw.TryRLock() {
		t.Fatal("first reader ownership")
	}
	if !rw.TryRLock() || rw.TryLock() {
		t.Fatal("reader ownership")
	}
	rw.RUnlock()
	rw.RUnlock()
	if !rw.TryLock() || rw.TryLock() || rw.TryRLock() {
		t.Fatal("writer ownership")
	}
	rw.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(rw.LockContext(ctx), context.Canceled) || !errors.Is(rw.RLockContext(ctx), context.Canceled) || !rw.TryLock() {
		t.Fatal("cancelled entry changed ownership")
	}
	rw.Unlock()
	if err := rw.RLockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	rw.RUnlock()
	if err := rw.LockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	rw.Unlock()
}

func TestRWMutexMisusePanics(t *testing.T) {
	var nilContext context.Context
	for _, f := range []func(){func() { var rw RWMutex; rw.Unlock() }, func() { var rw RWMutex; rw.RUnlock() }, func() { var rw RWMutex; _ = rw.LockContext(nilContext) }, func() { var rw RWMutex; _ = rw.RLockContext(nilContext) }, func() { var rw RWMutex; rw.Lock(); rw.RUnlock() }, func() { var rw RWMutex; rw.RLock(); rw.Unlock() }} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("RW misuse did not panic")
				}
			}()
			f()
		}()
	}
}

func waitRWPending(t *testing.T, rw *RWMutex, queuedReaders int) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		rw.guard.Lock()
		pending := rw.pending
		n := 0
		for p := rw.head; p != nil; p = p.next {
			n++
		}
		rw.guard.Unlock()
		if pending && n == queuedReaders {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("writer announcement/reader queue timed out")
}

func TestRWCancelledWriterReleasesReaders(t *testing.T) {
	var rw RWMutex
	rw.RLock()
	ctx, cancel := context.WithCancel(context.Background())
	writer := make(chan error, 1)
	reader := make(chan struct{})
	go func() { writer <- rw.LockContext(ctx) }()
	waitRWPending(t, &rw, 0)
	go func() { rw.RLock(); close(reader); rw.RUnlock() }()
	waitRWPending(t, &rw, 1)
	cancel()
	select {
	case err := <-writer:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer cancellation stalled")
	}
	select {
	case <-reader:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled writer retained queued readers")
	}
	rw.RUnlock()
	if !rw.TryLock() {
		t.Fatal("cancelled writer retained announcement")
	}
	rw.Unlock()
}

func TestRWPendingWriterBlocksFreshReaders(t *testing.T) {
	var rw RWMutex
	rw.RLock()
	writerAcquired := make(chan struct{})
	releaseWriter := make(chan struct{})
	readerAcquired := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { rw.Lock(); close(writerAcquired); <-releaseWriter; rw.Unlock() })
	waitRWPending(t, &rw, 0)
	if rw.TryRLock() {
		t.Fatal("TryRLock bypassed pending writer")
	}
	wg.Go(func() { rw.RLock(); close(readerAcquired); rw.RUnlock() })
	waitRWPending(t, &rw, 1)
	select {
	case <-readerAcquired:
		t.Fatal("fresh reader bypassed writer")
	default:
	}
	rw.RUnlock()
	<-writerAcquired
	select {
	case <-readerAcquired:
		t.Fatal("reader overlaps writer")
	default:
	}
	close(releaseWriter)
	wg.Wait()
	<-readerAcquired
}

func TestRWCancelledReaderLeavesNoReservation(t *testing.T) {
	var rw RWMutex
	rw.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- rw.RLockContext(ctx) }()
	waitRWPending(t, &rw, 1)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	rw.Unlock()
	if !rw.TryLock() {
		t.Fatal("cancelled reader retained count")
	}
	rw.Unlock()
}

func TestRWMutexExclusionAndOrdering(t *testing.T) {
	var rw RWMutex
	var wg sync.WaitGroup
	value := 0
	for actor := 0; actor < 8; actor++ {
		wg.Go(func() {
			for i := 0; i < 500; i++ {
				if i%5 == 0 {
					rw.Lock()
					value++
					rw.Unlock()
				} else {
					rw.RLock()
					_ = value
					rw.RUnlock()
				}
			}
		})
	}
	wg.Wait()
	if value != 800 {
		t.Fatal(value)
	}
}

func TestRWMutexContextStarvationFreedom(t *testing.T) {
	for _, readerVictim := range []bool{false, true} {
		t.Run(map[bool]string{false: "writer", true: "reader"}[readerVictim], func(t *testing.T) {
			var rw RWMutex
			var wg sync.WaitGroup
			stop := make(chan struct{})
			var calls atomic.Int64
			for i := 0; i < 4; i++ {
				wg.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
						}
						if readerVictim {
							rw.Lock()
							calls.Add(1)
							rw.Unlock()
						} else {
							rw.RLock()
							calls.Add(1)
							rw.RUnlock()
						}
					}
				})
			}
			defer func() { close(stop); wg.Wait() }()
			for calls.Load() < 100 {
				runtime.Gosched()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if readerVictim {
				if err := rw.RLockContext(ctx); err != nil {
					t.Fatal(err)
				}
				rw.RUnlock()
			} else {
				if err := rw.LockContext(ctx); err != nil {
					t.Fatal(err)
				}
				rw.Unlock()
			}
		})
	}
}

func TestRWUncontendedAllocations(t *testing.T) {
	var rw RWMutex
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	value := 0
	for name, run := range map[string]func(){"reader": func() { rw.RLock(); value++; rw.RUnlock() }, "writer": func() { rw.Lock(); value++; rw.Unlock() }, "try-reader": func() {
		if !rw.TryRLock() {
			panic("try reader")
		}
		rw.RUnlock()
	}, "try-writer": func() {
		if !rw.TryLock() {
			panic("try writer")
		}
		rw.Unlock()
	}, "context-reader": func() {
		if err := rw.RLockContext(ctx); err != nil {
			panic(err)
		}
		rw.RUnlock()
	}, "context-writer": func() {
		if err := rw.LockContext(ctx); err != nil {
			panic(err)
		}
		rw.Unlock()
	}} {
		t.Run(name, func(t *testing.T) {
			if n := allocationTotal(t, 1000, run); n != 0 {
				t.Fatalf("uncontended path made %d total allocations", n)
			}
		})
	}
}

func TestRWMixedCancellationStress(t *testing.T) {
	var rw RWMutex
	var wg sync.WaitGroup
	value := 0
	var expected atomic.Int64
	cancellations := make(chan context.CancelFunc, 8)
	cancelled := make(chan struct{})
	go func() {
		defer close(cancelled)
		for cancel := range cancellations {
			cancel()
		}
	}()
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for i := 0; i < 200; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				cancellations <- cancel
				switch (i + worker) % 6 {
				case 0:
					rw.Lock()
					value++
					expected.Add(1)
					rw.Unlock()
				case 1:
					rw.RLock()
					_ = value
					rw.RUnlock()
				case 2:
					if rw.TryLock() {
						value++
						expected.Add(1)
						rw.Unlock()
					}
				case 3:
					if rw.TryRLock() {
						_ = value
						rw.RUnlock()
					}
				case 4:
					if err := rw.LockContext(ctx); err == nil {
						value++
						expected.Add(1)
						rw.Unlock()
					} else if !errors.Is(err, context.Canceled) {
						panic(err)
					}
				case 5:
					if err := rw.RLockContext(ctx); err == nil {
						_ = value
						rw.RUnlock()
					} else if !errors.Is(err, context.Canceled) {
						panic(err)
					}
				}
				cancel()
			}
		})
	}
	wg.Wait()
	close(cancellations)
	<-cancelled
	if int64(value) != expected.Load() || !rw.TryLock() {
		t.Fatal("mixed traffic lost writes or ownership")
	}
	rw.Unlock()
}
