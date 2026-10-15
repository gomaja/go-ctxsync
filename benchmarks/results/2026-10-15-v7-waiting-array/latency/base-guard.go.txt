// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import (
	"runtime"
	"sync/atomic"
)

const guardTailUnit uint64 = 1 << 32
const guardPollBudget = 64

var testHookGuardYield func(*ticketGuard, int)

// ticketGuard admits short accounting sections in ticket order. Packing both
// counters makes TryLock's snapshot safe across uint32 wrap. The protocol
// requires fewer than 2^32 outstanding guard claims. No operation allocates
// or starts a goroutine; see modelcheck/guard.go for the atomic model.
// Waiting claims poll at most 64 mismatches before yielding. The model permits
// arbitrary fresh polling; a native test checks the cooperative yield budget.
// See modelcheck/evidence/model-v5-guard-review.md.
type ticketGuard struct{ word atomic.Uint64 }

func (g *ticketGuard) Lock() {
	ticket := uint32(g.word.Add(guardTailUnit)>>32) - 1
	for spins := 0; uint32(g.word.Load()) != ticket; {
		spins++
		if spins < guardPollBudget {
			continue
		}
		if testHookGuardYield != nil {
			testHookGuardYield(g, spins)
		}
		runtime.Gosched()
		spins = 0
	}
}

var testHookGuardTry func(*ticketGuard)

func (g *ticketGuard) TryLock() bool {
	old := g.word.Load()
	if uint32(old) != uint32(old>>32) {
		return false
	}
	if testHookGuardTry != nil {
		testHookGuardTry(g)
	}
	return g.word.CompareAndSwap(old, old+guardTailUnit)
}

func (g *ticketGuard) Unlock() {
	for {
		old := g.word.Load()
		// Head wrap must not carry into the independently allocated tail.
		newWord := old&0xffffffff00000000 | uint64(uint32(old)+1)
		if g.word.CompareAndSwap(old, newWord) {
			return
		}
		// While this head is held, each competing caller can allocate only
		// one ticket, then waits. TryLock cannot allocate behind a held head.
		// Thus retries are finite under the outstanding-claim bound.
	}
}
