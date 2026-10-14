// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestContextDoneInitializationAllocations(t *testing.T) {
	skipRacePoolAllocations(t)
	for _, kind := range []string{"mutex", "rw-reader", "rw-writer"} {
		for _, cancelWait := range []bool{false, true} {
			for _, warmDone := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/cancel=%v/warmDone=%v", kind, cancelWait, warmDone), func(t *testing.T) {
					var m Mutex
					var rw RWMutex
					contexts := make([]context.Context, 100+allocationWarmup)
					cancels := make([]context.CancelFunc, len(contexts))
					for i := range contexts {
						contexts[i], cancels[i] = context.WithCancel(context.Background())
						if warmDone {
							_ = contexts[i].Done()
						}
					}
					defer func() {
						for _, c := range cancels {
							c()
						}
					}()
					jobs, results := make(chan int), make(chan error)
					stopped := make(chan struct{})
					go func() {
						defer close(stopped)
						for i := range jobs {
							var err error
							switch kind {
							case "mutex":
								err = m.LockContext(contexts[i])
								if err == nil {
									m.Unlock()
								}
							case "rw-reader":
								err = rw.RLockContext(contexts[i])
								if err == nil {
									rw.RUnlock()
								}
							case "rw-writer":
								err = rw.LockContext(contexts[i])
								if err == nil {
									rw.Unlock()
								}
							}
							results <- err
						}
					}()
					defer func() { close(jobs); <-stopped }()
					iteration := 0
					allocs := allocationTotal(t, 100, func() {
						switch kind {
						case "mutex":
							m.Lock()
						case "rw-reader":
							rw.Lock()
						case "rw-writer":
							rw.RLock()
						}
						jobs <- iteration
						if kind == "mutex" {
							waitMutexWaiters(t, &m, 1)
						} else {
							waitRWPending(t, &rw, map[bool]int{true: 1, false: 0}[kind == "rw-reader"])
						}
						unlock := func() {
							switch kind {
							case "mutex":
								m.Unlock()
							case "rw-reader":
								rw.Unlock()
							case "rw-writer":
								rw.RUnlock()
							}
						}
						if cancelWait {
							cancels[iteration]()
							if err := <-results; !errors.Is(err, context.Canceled) {
								panic(err)
							}
							unlock()
						} else {
							unlock()
							if err := <-results; err != nil {
								panic(err)
							}
						}
						iteration++
					})
					t.Logf("total allocations across 100 contended calls = %d", allocs)
					want := uint64(100)
					if warmDone {
						want = 0
					}
					if allocs != want {
						t.Fatalf("got %d total allocations, want %d", allocs, want)
					}
				})
			}
		}
	}
}
