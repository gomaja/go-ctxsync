// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

// normalizationWitness is the seeded historical regression from
// go-sctp/sendlock_linux_test.go: TestSendLockStarvationExit. It is labeled a
// stronger idle-normalization check, not a claim that the stale flag alone
// violates P4's single-fresh-acquisition condition. The initial stale wake
// token is intentionally retained; an idle wake token is permitted.
func normalizationWitness(v variant) result {
	c := config{Name: "seeded/historical-starvation-exit", Actors: 1, Ops: 1, IdleAtExit: true}
	s := initial(c)
	s.Bits = starvingBit | wokenBit | waiterUnit
	s.Abandoned, s.Published, s.Wake[1] = 1, 1, true
	s.G[0] = actor{PC: unlockCheck, Old: s.Bits, ReleaseStarved: true}
	return explore(c, v, s, 10000)
}
