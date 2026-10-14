//go:build race

// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import "testing"

func skipRacePoolAllocations(t *testing.T) {
	t.Helper()
	// Go 1.26.9 sync/pool.go, Pool.Put, randomly discards one quarter of
	// entries under race instrumentation, invalidating warmed-pool counts.
	t.Skip("pooled allocation counts require a run without race instrumentation")
}
