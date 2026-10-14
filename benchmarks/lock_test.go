// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//lint:file-ignore SA2001 Benchmarks intentionally measure lock overhead with empty critical sections.

package benchmarks

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ctxsync "github.com/gomaja/go-ctxsync"
	"golang.org/x/sync/semaphore"
)

type mutexBenchmark interface {
	Lock()
	TryLock() bool
	LockContext(context.Context) error
	Unlock()
}

type rwBenchmark interface {
	mutexBenchmark
	RLock()
	TryRLock() bool
	RLockContext(context.Context) error
	RUnlock()
}

// sync's context adapters measure an entry check plus ordinary locking. They
// cannot cancel an operation once it starts waiting; cancellation tests exclude
// this overhead baseline. See the standard-library sync.Mutex and RWMutex APIs.
type syncMutex struct{ sync.Mutex }

func (m *syncMutex) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.Lock()
	return nil
}

type syncRWMutex struct{ sync.RWMutex }

func (m *syncRWMutex) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.Lock()
	return nil
}

func (m *syncRWMutex) RLockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.RLock()
	return nil
}

// Weighted's Acquire/Release pair models a mutex at capacity one. See
// golang.org/x/sync v0.24.0 semaphore/semaphore.go, Acquire and TryAcquire.
type semaphoreMutex struct{ sem *semaphore.Weighted }

func (m *semaphoreMutex) Lock()         { mustAcquire(m.LockContext(context.Background())) }
func (m *semaphoreMutex) Unlock()       { m.sem.Release(1) }
func (m *semaphoreMutex) TryLock() bool { return m.sem.TryAcquire(1) }
func (m *semaphoreMutex) LockContext(ctx context.Context) error {
	return m.sem.Acquire(ctx, 1)
}

// semaphore/semaphore.go:159-164 documents the reader weight-one/writer
// weight-capacity pattern. Capacity equals the benchmark worker count, so it
// does not limit the number of simultaneous readers in this workload.
type semaphoreRWMutex struct {
	sem      *semaphore.Weighted
	capacity int64
}

func (m *semaphoreRWMutex) Lock()         { mustAcquire(m.LockContext(context.Background())) }
func (m *semaphoreRWMutex) Unlock()       { m.sem.Release(m.capacity) }
func (m *semaphoreRWMutex) TryLock() bool { return m.sem.TryAcquire(m.capacity) }
func (m *semaphoreRWMutex) LockContext(ctx context.Context) error {
	return m.sem.Acquire(ctx, m.capacity)
}
func (m *semaphoreRWMutex) RLock()         { mustAcquire(m.RLockContext(context.Background())) }
func (m *semaphoreRWMutex) RUnlock()       { m.sem.Release(1) }
func (m *semaphoreRWMutex) TryRLock() bool { return m.sem.TryAcquire(1) }
func (m *semaphoreRWMutex) RLockContext(ctx context.Context) error {
	return m.sem.Acquire(ctx, 1)
}

func mustAcquire(err error) {
	if err != nil {
		panic(err)
	}
}

// retryMutex is the benchmark-only TryLock-retry design: ordinary Lock callers
// park in sync.Mutex, while context callers compete after buffered wake tokens.
// The channel is allocated once by the factory. A token predating a wait is
// harmless: the caller consumes it and retries before parking again.
type retryMutex struct {
	mu   sync.Mutex
	wake chan struct{}
}

func (m *retryMutex) Lock()         { m.mu.Lock() }
func (m *retryMutex) TryLock() bool { return m.mu.TryLock() }
func (m *retryMutex) Unlock() {
	m.mu.Unlock()
	notify(m.wake)
}
func (m *retryMutex) LockContext(ctx context.Context) error {
	if ctx.Done() == nil {
		m.Lock()
		return nil
	}
	return retryAcquire(ctx, m.mu.TryLock, m.wake)
}

type retryRWMutex struct {
	mu       sync.RWMutex
	tryGuard sync.Mutex
	wake     chan struct{}
}

func (m *retryRWMutex) Lock()          { m.mu.Lock() }
func (m *retryRWMutex) TryLock() bool  { return m.tryWrite(m.mu.TryLock) }
func (m *retryRWMutex) RLock()         { m.mu.RLock() }
func (m *retryRWMutex) TryRLock() bool { return m.mu.TryRLock() }
func (m *retryRWMutex) tryWrite(try func() bool) bool {
	// sync.RWMutex.TryLock may acquire its internal writer mutex, fail the
	// reader-count CAS, and release that mutex without a public Unlock. See
	// Go 1.26.9 sync/rwmutex.go:169-192. Serializing writer attempts ensures a
	// retry awakened by the last reader cannot fail against that transient
	// ownership and then park after its unnotified release.
	m.tryGuard.Lock()
	defer m.tryGuard.Unlock()
	return try()
}
func (m *retryRWMutex) Unlock() {
	m.mu.Unlock()
	notify(m.wake)
}
func (m *retryRWMutex) RUnlock() {
	m.mu.RUnlock()
	notify(m.wake)
}
func (m *retryRWMutex) LockContext(ctx context.Context) error {
	if ctx.Done() == nil {
		m.Lock()
		return nil
	}
	return retryAcquire(ctx, m.TryLock, m.wake)
}
func (m *retryRWMutex) RLockContext(ctx context.Context) error {
	if ctx.Done() == nil {
		m.RLock()
		return nil
	}
	return retryAcquire(ctx, m.mu.TryRLock, m.wake)
}

func notify(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

func retryAcquire(ctx context.Context, try func() bool, wake <-chan struct{}) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if try() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

var mutexFactories = []struct {
	name string
	new  func() mutexBenchmark
}{
	{"ctxsync", func() mutexBenchmark { return new(ctxsync.Mutex) }},
	{"sync", func() mutexBenchmark { return new(syncMutex) }},
	{"semaphore", func() mutexBenchmark { return &semaphoreMutex{sem: semaphore.NewWeighted(1)} }},
	{"retry", func() mutexBenchmark { return &retryMutex{wake: make(chan struct{}, 1)} }},
}

var rwFactories = []struct {
	name string
	new  func() rwBenchmark
}{
	{"ctxsync", func() rwBenchmark { return new(ctxsync.RWMutex) }},
	{"sync", func() rwBenchmark { return new(syncRWMutex) }},
	{"semaphore", func() rwBenchmark {
		capacity := int64(runtime.GOMAXPROCS(0))
		return &semaphoreRWMutex{sem: semaphore.NewWeighted(capacity), capacity: capacity}
	}},
	{"retry-serialized", func() rwBenchmark { return &retryRWMutex{wake: make(chan struct{}, 1)} }},
}

func benchmarkContext(b *testing.B, cancellable bool) context.Context {
	if !cancellable {
		return context.Background()
	}
	ctx, cancel := context.WithCancel(context.Background())
	// cancelCtx creates its Done channel lazily. Allocate it during setup so
	// comparator timing includes acquisition rather than context initialization.
	_ = ctx.Done()
	b.Cleanup(cancel)
	return ctx
}

//nolint:staticcheck // Empty critical sections measure the lock operations themselves.
func BenchmarkMutexDirectLockUnlock(b *testing.B) {
	b.Run("impl=sync", func(b *testing.B) {
		var m sync.Mutex
		m.Lock()
		m.Unlock()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			m.Lock()
			m.Unlock()
		}
	})
	b.Run("impl=ctxsync", func(b *testing.B) {
		var m ctxsync.Mutex
		m.Lock()
		m.Unlock()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			m.Lock()
			m.Unlock()
		}
	})
}

//nolint:staticcheck // Empty critical sections measure the lock operations themselves.
func BenchmarkMutexUncontended(b *testing.B) {
	for _, operation := range []string{"LockUnlock", "TryLock", "ContextBackground", "ContextCancellable"} {
		b.Run("operation="+operation, func(b *testing.B) {
			for _, factory := range mutexFactories {
				b.Run("impl="+factory.name, func(b *testing.B) {
					m := factory.new()
					ctx := benchmarkContext(b, operation == "ContextCancellable")
					m.Lock()
					m.Unlock()
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						switch operation {
						case "LockUnlock":
							m.Lock()
						case "TryLock":
							if !m.TryLock() {
								b.Fatal("uncontended TryLock failed")
							}
						default:
							if err := m.LockContext(ctx); err != nil {
								b.Fatal(err)
							}
						}
						m.Unlock()
					}
				})
			}
		})
	}
}

//nolint:staticcheck // Empty critical sections measure the lock operations themselves.
func BenchmarkMutexParallel(b *testing.B) {
	for _, operation := range []string{"LockUnlock", "ContextBackground", "ContextCancellable"} {
		b.Run("operation="+operation, func(b *testing.B) {
			for _, factory := range mutexFactories {
				b.Run("impl="+factory.name, func(b *testing.B) {
					m := factory.new()
					ctx := benchmarkContext(b, operation == "ContextCancellable")
					m.Lock()
					m.Unlock()
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if operation == "LockUnlock" {
								m.Lock()
							} else if err := m.LockContext(ctx); err != nil {
								panic(err)
							}
							m.Unlock()
						}
					})
				})
			}
		})
	}
}

//nolint:staticcheck // Empty critical sections measure the lock operations themselves.
func BenchmarkRWParallel(b *testing.B) {
	for _, mix := range []struct {
		name  string
		reads uint32
	}{{"90r10w", 9}, {"10r90w", 1}} {
		b.Run("mix="+mix.name, func(b *testing.B) {
			for _, operation := range []string{"Plain", "ContextBackground", "ContextCancellable"} {
				b.Run("operation="+operation, func(b *testing.B) {
					for _, factory := range rwFactories {
						b.Run("impl="+factory.name, func(b *testing.B) {
							m := factory.new()
							ctx := benchmarkContext(b, operation == "ContextCancellable")
							m.Lock()
							m.Unlock()
							m.RLock()
							m.RUnlock()
							var worker atomic.Uint32
							b.ReportAllocs()
							b.ResetTimer()
							b.RunParallel(func(pb *testing.PB) {
								phase := (worker.Add(1) - 1) % 10
								for pb.Next() {
									// Every iteration contains exactly ten pairs, regardless
									// of the distribution of iterations among workers.
									for j := range uint32(10) {
										read := (j+phase)%10 < mix.reads
										if read {
											if operation == "Plain" {
												m.RLock()
											} else {
												mustAcquire(m.RLockContext(ctx))
											}
											m.RUnlock()
										} else {
											if operation == "Plain" {
												m.Lock()
											} else {
												mustAcquire(m.LockContext(ctx))
											}
											m.Unlock()
										}
									}
								}
							})
							b.ReportMetric(10, "locks/op")
						})
					}
				})
			}
		})
	}
}

func TestComparatorCancellationAtEntry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, factory := range mutexFactories {
		t.Run(factory.name, func(t *testing.T) {
			m := factory.new()
			if err := m.LockContext(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled entry = %v", err)
			}
			if !m.TryLock() {
				t.Fatal("cancelled call retained ownership")
			}
			m.Unlock()
		})
	}
}

func TestRetryWaitCancellation(t *testing.T) {
	m := &retryMutex{wake: make(chan struct{}, 1)}
	m.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- m.LockContext(ctx) }()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting cancellation = %v", err)
	}
	if m.TryLock() {
		t.Fatal("cancelling waiter released the owner's lock")
	}
	m.Unlock()
	if !m.TryLock() {
		t.Fatal("owner's unlock did not release the lock")
	}
	m.Unlock()
}

func TestRetryNotificationAfterFailedAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wake := make(chan struct{}, 1)
	firstAttempt := make(chan struct{})
	var attempted sync.Once
	var available atomic.Bool
	try := func() bool {
		if available.CompareAndSwap(true, false) {
			return true
		}
		attempted.Do(func() { close(firstAttempt) })
		return false
	}
	result := make(chan error, 1)
	go func() { result <- retryAcquire(ctx, try, wake) }()
	<-firstAttempt
	available.Store(true)
	notify(wake)
	if err := <-result; err != nil {
		t.Fatalf("release between failed TryLock and waiting was lost: %v", err)
	}
	if available.Load() {
		t.Fatal("retry returned without consuming ownership")
	}
}

func TestRetryWakeAndOwnership(t *testing.T) {
	m := &retryMutex{wake: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var count int
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 100 {
				mustAcquire(m.LockContext(ctx))
				count++
				m.Unlock()
			}
		})
	}
	workers.Wait()
	if count != 800 {
		t.Fatalf("completed protected increments = %d, want 800", count)
	}
}

func TestRWComparatorOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, factory := range rwFactories {
		t.Run(factory.name, func(t *testing.T) {
			m := factory.new()
			m.Lock()
			if m.TryRLock() || m.TryLock() {
				t.Fatal("writer did not exclude another acquisition")
			}
			m.Unlock()
			m.RLock()
			if m.TryLock() {
				t.Fatal("reader did not exclude a writer")
			}
			m.RUnlock()
			if err := m.RLockContext(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled read entry = %v", err)
			}
			if !m.TryLock() {
				t.Fatal("cancelled reader retained a reservation")
			}
			m.Unlock()
		})
	}
}

func TestRetryRWWakeAndOwnership(t *testing.T) {
	m := &retryRWMutex{wake: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var count int
	var workers sync.WaitGroup
	for id := range 8 {
		workers.Go(func() {
			for range 100 {
				if id%2 == 0 {
					mustAcquire(m.LockContext(ctx))
					count++
					m.Unlock()
				} else {
					mustAcquire(m.RLockContext(ctx))
					if count < 0 || count > 400 {
						panic("invalid protected counter")
					}
					m.RUnlock()
				}
			}
		})
	}
	workers.Wait()
	if count != 400 {
		t.Fatalf("completed protected writer increments = %d, want 400", count)
	}
}

func TestRetryRWSerializesWriteAttempts(t *testing.T) {
	var m retryRWMutex
	m.tryWrite(func() bool {
		if m.tryGuard.TryLock() {
			m.tryGuard.Unlock()
			t.Error("a writer TryLock attempt ran outside the serialization gate")
		}
		return false
	})
}

func TestNotificationOnlyRetryCanStrandAfterTransientWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	var reader, internalWriter atomic.Bool
	reader.Store(true)
	firstFailure := make(chan struct{})
	transientHeld := make(chan struct{})
	retryFailed := make(chan struct{})
	releaseTransient := make(chan struct{})
	transientReleased := make(chan struct{})
	var aCalls, bCalls atomic.Int32
	results := make(chan error, 2)

	// A reader prevents the first attempt. Its last public RUnlock later
	// supplies the only notification, while another writer attempt still
	// holds a transient internal writer lock after its reader-count failure.
	go func() {
		err := retryAcquire(ctx, func() bool {
			n := bCalls.Add(1)
			if n == 1 {
				close(firstFailure)
				return false
			}
			if reader.Load() || !internalWriter.CompareAndSwap(false, true) {
				close(retryFailed)
				return false
			}
			return true
		}, wake)
		results <- err
	}()
	<-firstFailure
	go func() {
		err := retryAcquire(ctx, func() bool {
			if aCalls.Add(1) == 1 {
				internalWriter.Store(true)
				close(transientHeld)
				<-releaseTransient
				internalWriter.Store(false)
				close(transientReleased)
				return false
			}
			return !reader.Load() && internalWriter.CompareAndSwap(false, true)
		}, wake)
		results <- err
	}()
	<-transientHeld
	reader.Store(false)
	notify(wake)
	<-retryFailed
	close(releaseTransient)
	<-transientReleased
	if reader.Load() || internalWriter.Load() || len(wake) != 0 {
		t.Fatal("synthetic lock must be ownerless with its notification consumed")
	}
	select {
	case err := <-results:
		t.Fatalf("notification-only wait unexpectedly completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	for range 2 {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("stranded wait did not remain parked until cancellation: %v", err)
		}
	}
	if aCalls.Load() != 1 || bCalls.Load() != 2 {
		t.Fatal("an unnotified transient release must not trigger another attempt")
	}
}

func TestSerializedRetryCompletesAfterTransientWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m := &retryRWMutex{wake: make(chan struct{}, 1)}
	var reader, internalWriter atomic.Bool
	reader.Store(true)
	firstFailure := make(chan struct{})
	transientHeld := make(chan struct{})
	retryStarted := make(chan struct{})
	releaseTransient := make(chan struct{})
	var aCalls, bCalls atomic.Int32
	results := make(chan error, 2)
	finish := func(err error) {
		if err == nil {
			internalWriter.Store(false)
			notify(m.wake)
		}
		results <- err
	}
	go func() {
		finish(retryAcquire(ctx, func() bool {
			n := bCalls.Add(1)
			if n == 2 {
				close(retryStarted)
			}
			return m.tryWrite(func() bool {
				if n == 1 {
					close(firstFailure)
					return false
				}
				return !reader.Load() && internalWriter.CompareAndSwap(false, true)
			})
		}, m.wake))
	}()
	<-firstFailure
	go func() {
		finish(retryAcquire(ctx, func() bool {
			return m.tryWrite(func() bool {
				if aCalls.Add(1) == 1 {
					internalWriter.Store(true)
					close(transientHeld)
					<-releaseTransient
					internalWriter.Store(false)
					return false
				}
				return !reader.Load() && internalWriter.CompareAndSwap(false, true)
			})
		}, m.wake))
	}()
	<-transientHeld
	reader.Store(false)
	notify(m.wake)
	<-retryStarted
	close(releaseTransient)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("serialized retry did not complete after transient release: %v", err)
		}
	}
	if reader.Load() || internalWriter.Load() || aCalls.Load() < 2 || bCalls.Load() != 2 {
		t.Fatal("both retry callers must acquire and release the synthetic lock")
	}
}
