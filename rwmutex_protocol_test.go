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

func TestRWCancelledGrantedReaderUndoesCount(t *testing.T) {
	var rw RWMutex
	rw.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	readerResult := make(chan error, 1)
	paused, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	testHookRWReaderWake = func(rr *RWMutex, _ *waiter) {
		if rr == &rw {
			close(paused)
			<-resume
		}
	}
	t.Cleanup(func() { cancel(); once.Do(func() { close(resume) }); testHookRWReaderWake = nil })
	go func() { readerResult <- rw.RLockContext(ctx) }()
	waitRWPending(t, &rw, 1)
	rw.Unlock()
	<-paused
	writerCtx, writerCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer writerCancel()
	writerResult := make(chan error, 1)
	go func() {
		err := rw.LockContext(writerCtx)
		if err == nil {
			rw.Unlock()
		}
		writerResult <- err
	}()
	waitRWPending(t, &rw, 0)
	cancel()
	once.Do(func() { close(resume) })
	if err := <-readerResult; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-writerResult; err != nil {
		t.Fatal("cancelled granted reader retained its reserved count:", err)
	}
}

func TestRWCancelledWriterAfterDrainReleasesReaders(t *testing.T) {
	var rw RWMutex
	rw.RLock()
	ctx, cancel := context.WithCancel(context.Background())
	writerResult := make(chan error, 1)
	paused, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	testHookRWWriterWake = func(rr *RWMutex) {
		if rr == &rw {
			close(paused)
			<-resume
		}
	}
	t.Cleanup(func() { cancel(); once.Do(func() { close(resume) }); testHookRWWriterWake = nil })
	go func() { writerResult <- rw.LockContext(ctx) }()
	waitRWPending(t, &rw, 0)
	readerDone := make(chan struct{})
	go func() {
		rw.RLock()
		if rw.TryLock() {
			panic("writer acquired during reader ownership")
		}
		rw.RUnlock()
		close(readerDone)
	}()
	waitRWPending(t, &rw, 1)
	rw.RUnlock()
	<-paused
	cancel()
	once.Do(func() { close(resume) })
	if err := <-writerResult; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-readerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled drained writer retained readers")
	}
	if !rw.TryLock() {
		t.Fatal("drain cancellation left ownership")
	}
	rw.Unlock()
}

func TestRWReleasedReaderBatchPrecedesNextWriter(t *testing.T) {
	var rw RWMutex
	rw.Lock()
	paused, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	testHookRWReaderWake = func(rr *RWMutex, _ *waiter) {
		if rr == &rw {
			close(paused)
			<-resume
		}
	}
	var wg sync.WaitGroup
	t.Cleanup(func() { once.Do(func() { close(resume) }); wg.Wait(); testHookRWReaderWake = nil })
	readerHeld := make(chan struct{})
	releaseReader := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseReader) }) })
	wg.Go(func() { rw.RLock(); close(readerHeld); <-releaseReader; rw.RUnlock() })
	waitRWPending(t, &rw, 1)
	rw.Unlock()
	<-paused
	writerHeld := make(chan struct{})
	wg.Go(func() { rw.Lock(); close(writerHeld); rw.Unlock() })
	waitRWPending(t, &rw, 0)
	select {
	case <-writerHeld:
		t.Fatal("writer skipped a reserved reader batch")
	default:
	}
	once.Do(func() { close(resume) })
	<-readerHeld
	select {
	case <-writerHeld:
		t.Fatal("writer overlaps released reader")
	default:
	}
	releaseOnce.Do(func() { close(releaseReader) })
}

func TestRWWarmContendedAllocations(t *testing.T) {
	skipRacePoolAllocations(t)
	for _, reader := range []bool{true, false} {
		t.Run(map[bool]string{true: "reader", false: "writer"}[reader], func(t *testing.T) {
			var rw RWMutex
			jobs := make(chan struct{})
			done := make(chan struct{})
			stopped := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_ = ctx.Done()
			go func() {
				defer close(stopped)
				for range jobs {
					if reader {
						if err := rw.RLockContext(ctx); err != nil {
							panic(err)
						}
						rw.RUnlock()
					} else {
						if err := rw.LockContext(ctx); err != nil {
							panic(err)
						}
						rw.Unlock()
					}
					done <- struct{}{}
				}
			}()
			defer func() { close(jobs); <-stopped }()
			allocs := allocationTotal(t, 100, func() {
				if reader {
					rw.Lock()
					jobs <- struct{}{}
					waitRWPending(t, &rw, 1)
					rw.Unlock()
				} else {
					rw.RLock()
					jobs <- struct{}{}
					waitRWPending(t, &rw, 0)
					rw.RUnlock()
				}
				<-done
			})
			if allocs != 0 {
				t.Fatal("warm RW wait allocated:", allocs)
			}
		})
	}
}

func TestRWWarmCancellationAllocations(t *testing.T) {
	skipRacePoolAllocations(t)
	for _, reader := range []bool{true, false} {
		t.Run(map[bool]string{true: "reader", false: "writer"}[reader], func(t *testing.T) {
			var rw RWMutex
			contexts := make([]context.Context, 100+allocationWarmup)
			cancels := make([]context.CancelFunc, len(contexts))
			for i := range contexts {
				contexts[i], cancels[i] = context.WithCancel(context.Background())
				_ = contexts[i].Done()
			}
			defer func() {
				for _, cancel := range cancels {
					cancel()
				}
			}()
			jobs, results := make(chan int), make(chan error)
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				for i := range jobs {
					if reader {
						results <- rw.RLockContext(contexts[i])
					} else {
						results <- rw.LockContext(contexts[i])
					}
				}
			}()
			defer func() { close(jobs); <-stopped }()
			i := 0
			allocs := allocationTotal(t, 100, func() {
				if reader {
					rw.Lock()
				} else {
					rw.RLock()
				}
				jobs <- i
				if reader {
					waitRWPending(t, &rw, 1)
				} else {
					waitRWPending(t, &rw, 0)
				}
				cancels[i]()
				if err := <-results; !errors.Is(err, context.Canceled) {
					panic("cancellation retained ownership")
				}
				if reader {
					rw.Unlock()
				} else {
					rw.RUnlock()
				}
				i++
			})
			if allocs != 0 {
				t.Fatal("warm RW cancellation allocated:", allocs)
			}
		})
	}
}
