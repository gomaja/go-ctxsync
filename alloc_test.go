// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
)

const allocationWarmup = 1

var warmContextErr sync.Once

// allocationTotal measures aggregate allocations, including those made by
// workers synchronized with f. Callers must not run in parallel. Go 1.26.9
// testing/allocs.go rounds averages down; totals retain fractional allocations.
func allocationTotal(t *testing.T, runs int, f func()) uint64 {
	t.Helper()
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	// Preserve the warmed pool during measurement; a GC may empty it.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	warmContextErr.Do(func() {
		// Go 1.26.9 context/context.go, cancelCtx.Err, asserts the stored
		// interface to error. runtime/iface.go, typeAssert, lazily allocates
		// its type cache. Warm context methods outside measurement.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for range 1 << 20 {
			if ctx.Err() != context.Canceled {
				t.Fatal("context warm-up")
			}
		}
	})
	for i := 0; i < allocationWarmup; i++ {
		f()
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		f()
	}
	runtime.ReadMemStats(&after)
	return after.Mallocs - before.Mallocs
}
