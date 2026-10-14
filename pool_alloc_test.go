//go:build !race

// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import "testing"

func skipRacePoolAllocations(t *testing.T) { t.Helper() }
