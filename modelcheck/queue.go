// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Go-derived mutex mode policy is covered by GO_LICENSE.
// Copyright 2024 The Go Authors. All rights reserved.

package main

const (
	requeueTail          variant = "requeue-tail"
	exitIgnoresAge       variant = "exit-ignores-age"
	dropHandedOwnership  variant = "cancel-drops-handoff"
	leaveCancelledNode   variant = "cancel-leaves-node"
	suppressWakeHeld     variant = "suppress-wake-while-held"
	cancelWokenNoAdvance variant = "cancel-woken-without-advance"
	probeIgnoresStarve   variant = "probe-ignores-starvation"
	probeDropsAccounting variant = "probe-drops-accounting"
	probeRetries         variant = "probe-retries-cas-loss"
)

type queuePhase uint8

const (
	qEntry queuePhase = iota
	qFast
	qTryLoad
	qTryCAS
	qProbeCheck
	qProbeLoad
	qProbeCAS
	qInitLoad
	qAllocate
	qPublish
	qReload
	qTake
	qInspect
	qCAS
	qRegisterAdd
	qEnqueue
	qProactiveWake
	qWait
	qCancelAdd
	qRemove
	qAdopt
	qCritical
	qUnlockAdd
	qUnlockTake
	qUnlockInspect
	qWakeAdd
	qDequeue
	qSend
	qDone
)

var queuePhaseNames = [...]string{"entry", "fast-CAS", "try-load", "try-CAS", "probe-context-check", "probe-load", "probe-CAS", "init-load", "allocate", "publish", "reload", "queue-take", "inspect", "acquire-CAS", "register-Add", "enqueue", "registration-wake", "wait", "cancel-Add", "remove", "adopt-Add", "critical", "unlock-Add", "unlock-queue-take", "unlock-inspect", "wake-Add", "dequeue", "send", "done"}

func (p queuePhase) String() string { return queuePhaseNames[p] }

// New is a CAS target in qCAS or qProbeCAS and a fixed Add delta in cancellation,
// adoption, and wake phases. Release preserves the Add result's mode and, for
// registration, whether an owner already owes the queue a wake.
type queueActor struct {
	Old, New, Release                                               int16
	PC                                                              queuePhase
	Op, Pair, Target, Mode, SendMode                                uint8
	Cancelled, Started, Aged, Owns, Signal, ReturnError, ResumeWait bool
}

type waiterQueue struct {
	Order       [4]uint8
	Guard, Hand uint8
}

type queueState struct {
	Bits      int16
	Published uint8
	Queues    [5]waiterQueue
	G         [4]queueActor
}

type queueEdge struct {
	Next        queueState
	Actor       uint8
	Action      action
	Note, Error string
	Cover       uint64
}

const (
	qCoverHead uint64 = 1 << iota
	qCoverCancelHead
	qCoverCancelHand
	qCoverCancelWake
	qCoverEntry
	qCoverExit
	qCoverBarge
	qCoverInitLoser
	qCoverHand
)

var queueCoverage = [...]string{"head requeue", "cancel queue head", "cancel handed ownership", "cancel normal wake", "starvation entry", "starvation exit", "normal barging", "lazy publication loser", "targeted handoff"}

func queueInitial(c config) queueState {
	s := queueState{}
	for i := c.Actors; i < len(s.G); i++ {
		s.G[i].PC = qDone
	}
	return s
}

func (s *queueState) enqueue(pair, id uint8, head bool) {
	q := &s.Queues[pair]
	if head {
		for i := len(q.Order) - 1; i > 0; i-- {
			q.Order[i] = q.Order[i-1]
		}
		q.Order[0] = id
		return
	}
	for i := range q.Order {
		if q.Order[i] == 0 {
			q.Order[i] = id
			return
		}
	}
	panic("model queue overflow")
}

func (s *queueState) remove(pair, id uint8) bool {
	q := &s.Queues[pair]
	for i, v := range q.Order {
		if v != id {
			continue
		}
		copy(q.Order[i:], q.Order[i+1:])
		q.Order[len(q.Order)-1] = 0
		return true
	}
	return false
}

func queueComplete(s *queueState, i int, c config) {
	g := s.G[i]
	if g.Pair != 0 && s.Queues[g.Pair].Guard == uint8(i+1) {
		s.Queues[g.Pair].Guard = 0
	}
	op := g.Op + 1
	s.G[i] = queueActor{Op: op}
	if c.Repeat[i] {
		s.G[i] = queueActor{}
	} else if int(op) >= c.Ops {
		s.G[i].PC = qDone
	}
}

func queueNormalize(s queueState) queueState {
	for i := range s.G {
		g := &s.G[i]
		if g.PC != qCAS && g.PC != qCancelAdd && g.PC != qWakeAdd && g.PC != qAdopt && g.PC != qTryCAS && g.PC != qProbeCAS {
			g.Old, g.New = 0, 0
		}
		if g.PC < qUnlockTake && !g.ResumeWait {
			g.Release = 0
		}
		if g.Owns && !g.ReturnError {
			g.Cancelled, g.Started, g.Aged = false, false, false
		}
	}
	return s
}

func queueInvariant(s queueState) string {
	owners := 0
	for _, g := range s.G {
		if g.Owns {
			owners++
		}
	}
	if owners > 1 {
		return "P1: multiple owners"
	}
	if s.Bits < 0 {
		return "P1: negative state"
	}
	for _, q := range s.Queues {
		if q.Hand != 0 && s.G[q.Hand-1].Owns {
			return "P1: handoff overlaps an owner"
		}
		var ids uint8
		for _, id := range q.Order {
			if id == 0 {
				continue
			}
			if ids&(1<<(id-1)) != 0 {
				return "P1: duplicate queue node"
			}
			ids |= 1 << (id - 1)
			if s.G[id-1].PC == qDone {
				return "P3: cancelled operation retained a queue node"
			}
		}
	}
	return ""
}

func queueTerminal(s queueState, c config) bool {
	for i := 0; i < c.Actors; i++ {
		if s.G[i].PC != qDone {
			return false
		}
	}
	return true
}

func queueIdle(s queueState) bool {
	if s.Bits != 0 {
		return false
	}
	for _, q := range s.Queues {
		if q.Guard != 0 || q.Hand != 0 || q.Order != [4]uint8{} {
			return false
		}
	}
	for _, g := range s.G {
		if g.Signal || g.Owns {
			return false
		}
	}
	return true
}

func queueSuccessors(s queueState, c config, v variant) []queueEdge {
	out := make([]queueEdge, 0, c.Actors*3)
	for i := 0; i < c.Actors; i++ {
		g := s.G[i]
		if g.PC == qDone {
			continue
		}
		k := c.Kinds[i][g.Op]
		add := func(n queueState, a action, note string, cover uint64) {
			out = append(out, queueEdge{Next: queueNormalize(n), Actor: uint8(i), Action: a, Note: note, Cover: cover})
		}
		if k == contextLock && !g.Cancelled && g.PC < qCritical {
			n := s
			n.G[i].Cancelled = true
			add(n, cancelAction, "close Done", 0)
		}
		if queueAgeEnabled(g) {
			n := s
			n.G[i].Aged = true
			add(n, ageAction, "wait crosses 1 ms", 0)
		}
		n := s
		p := &n.G[i]
		a := localAction
		note := "queue " + g.PC.String()
		var cover uint64
		finishWake := func() {
			if g.ResumeWait {
				n.Queues[g.Pair].Guard = 0
				p.PC, p.ResumeWait, p.SendMode, p.Target, p.Release = qWait, false, 0, 0, 0
			} else {
				queueComplete(&n, i, c)
			}
		}
		switch g.PC {
		case qEntry:
			if g.Cancelled {
				queueComplete(&n, i, c)
			} else if k == tryLock {
				p.PC = qTryLoad
			} else {
				p.PC = qFast
			}
		case qFast:
			a = atomicAction
			note = "fast Lock CAS"
			if n.Bits == 0 {
				n.Bits = heldBit
				p.Owns = true
				p.PC = qCritical
			} else {
				p.PC = qProbeCheck
			}
		case qTryLoad:
			a = atomicAction
			p.Old = n.Bits
			mask := heldBit | starvingBit
			if v == tryIgnoresStarve {
				mask = heldBit
			}
			if p.Old&mask != 0 {
				queueComplete(&n, i, c)
			} else {
				p.PC = qTryCAS
			}
		case qTryCAS:
			a = atomicAction
			if n.Bits == g.Old {
				n.Bits |= heldBit
				p.Owns = true
				p.PC = qCritical
				if g.Old&starvingBit != 0 {
					out = append(out, queueEdge{Next: n, Actor: uint8(i), Error: "fairness: TryLock bypassed starvation"})
					continue
				}
			} else {
				queueComplete(&n, i, c)
			}
		case qProbeCheck:
			// The one fresh-call probe repeats the context check before its
			// load. Cancellation after this check may still race acquisition.
			if g.Cancelled {
				queueComplete(&n, i, c)
			} else {
				p.PC = qProbeLoad
			}
		case qProbeLoad:
			a = atomicAction
			p.Old = n.Bits
			mask := heldBit | starvingBit
			if v == probeIgnoresStarve {
				mask = heldBit
			}
			if p.Old&mask != 0 {
				p.PC = qInitLoad
			} else {
				p.New, p.PC = p.Old|heldBit, qProbeCAS
				if v == probeDropsAccounting {
					p.New = heldBit
				}
			}
		case qProbeCAS:
			a = atomicAction
			if n.Bits == g.Old {
				n.Bits = g.New
				p.Owns, p.PC = true, qCritical
				if g.Old&starvingBit != 0 {
					out = append(out, queueEdge{Next: n, Actor: uint8(i), Error: "fairness: fresh probe bypassed starvation"})
					continue
				}
				if g.Old/waiterUnit > 0 {
					cover = qCoverBarge
				}
			} else {
				// A failed probe always enters the existing guarded protocol.
				// Retrying here admits a fair pre-registration livelock.
				p.PC = qInitLoad
				if v == probeRetries {
					p.PC = qProbeCheck
				}
			}
		case qInitLoad, qReload:
			a = atomicAction
			p.Pair = n.Published
			if p.Pair == 0 {
				p.PC = qAllocate
			} else {
				p.PC = qTake
			}
		case qAllocate:
			a = allocateAction
			p.Pair = uint8(i + 1)
			p.PC = qPublish
		case qPublish:
			a = atomicAction
			note = "publish queue CAS"
			if n.Published == 0 || v == lazyPublish {
				n.Published = p.Pair
				p.PC = qTake
			} else {
				p.PC = qReload
				cover = qCoverInitLoser
			}
		case qTake, qUnlockTake:
			if n.Queues[g.Pair].Guard != 0 {
				continue
			}
			n.Queues[g.Pair].Guard = uint8(i + 1)
			if g.PC == qTake {
				p.PC = qInspect
			} else {
				p.PC = qUnlockInspect
			}
		case qInspect:
			a = atomicAction
			p.Old = n.Bits
			if g.Mode == 2 {
				p.New = heldBit - waiterUnit
				if !g.Aged || p.Old/waiterUnit == 1 || v == exitIgnoresAge {
					p.New -= starvingBit
					cover = qCoverExit
				}
				p.PC = qAdopt
			} else if g.Cancelled {
				p.New = 0
				if g.Mode == 1 {
					p.New -= wokenBit
					cover = qCoverCancelWake
				}
				for _, id := range n.Queues[g.Pair].Order {
					if id == uint8(i+1) {
						p.New -= waiterUnit
						if n.Queues[g.Pair].Order[0] == id {
							cover = qCoverCancelHead
						}
						break
					}
				}
				if (p.Old+p.New)/waiterUnit == 0 && p.Old&starvingBit != 0 {
					p.New -= starvingBit
				}
				p.PC = qCancelAdd
			} else if p.Old&(heldBit|starvingBit) == 0 {
				p.New = p.Old | heldBit
				if g.Mode == 1 {
					p.New &^= wokenBit
				}
				p.PC = qCAS
			} else {
				p.PC = qRegisterAdd
			}
		case qCAS:
			// Only one acquisition attempt is permitted while holding the
			// queue guard. A losing CAS must register instead of retrying.
			a = atomicAction
			if n.Bits != g.Old {
				p.PC = qRegisterAdd
			} else {
				n.Bits = g.New
				p.Owns, p.Signal, p.Mode = true, false, 0
				n.Queues[g.Pair].Guard = 0
				p.PC = qCritical
				if g.Old/waiterUnit > 0 {
					cover = qCoverBarge
				}
			}
		case qRegisterAdd:
			// The guard makes count, woken, and starvation fields stable.
			// Concurrent fast paths only toggle held, so this fixed delta
			// preserves their updates and cannot retry indefinitely.
			a = atomicAction
			delta := waiterUnit
			if g.Mode == 1 {
				delta -= wokenBit
			}
			if g.Aged && n.Bits&starvingBit == 0 {
				delta += starvingBit
				cover = qCoverEntry
			}
			n.Bits += delta
			p.Release = n.Bits
			p.Mode, p.ResumeWait, p.PC = 0, true, qEnqueue
		case qEnqueue:
			// Go 1.26.9 internal/sync/mutex.go, lockSlow lines 144-149:
			// queueLifo preserves priority after an unsuccessful normal wake.
			head := g.Started && v != requeueTail
			n.enqueue(g.Pair, uint8(i+1), head)
			if head {
				cover = qCoverHead
			}
			p.Started = true
			p.Mode = 0
			p.Signal = false
			p.ResumeWait = true
			p.PC = qProactiveWake
		case qWait:
			if !g.Signal && !g.Cancelled {
				continue
			}
			a = selectAction
			p.Signal = false
			p.PC = qTake
		case qCancelAdd:
			a = atomicAction
			n.Bits += g.New
			p.PC = qRemove
		case qRemove:
			if v != leaveCancelledNode {
				n.remove(g.Pair, uint8(i+1))
			}
			p.Signal = false
			p.Mode = 0
			p.ReturnError = true
			p.ResumeWait = false
			p.Release = n.Bits
			p.PC = qUnlockInspect
			if v == cancelWokenNoAdvance && g.Mode == 1 {
				queueComplete(&n, i, c)
			}
		case qAdopt:
			// Go's last-waiter/young-recipient starvation exit condition is
			// retained. Starvation excludes bargers, so adoption uses Add.
			a = atomicAction
			n.Bits += g.New
			n.Queues[g.Pair].Hand = 0
			p.Signal, p.Mode, p.Owns = false, 0, true
			n.Queues[g.Pair].Guard = 0
			if g.Cancelled {
				cover = qCoverCancelHand
				p.ReturnError, p.PC = true, qUnlockAdd
				if v == dropHandedOwnership {
					p.Owns = false
					queueComplete(&n, i, c)
				}
			} else {
				p.PC = qCritical
			}
		case qCritical:
			p.PC = qUnlockAdd
		case qUnlockAdd:
			a = atomicAction
			note = "Unlock Add and pinned mode snapshot"
			n.Bits -= heldBit
			p.Owns = false
			p.Release = n.Bits
			// A committed normal recipient owns advancement: acquisition
			// supplies a future release; cancellation or a lost race advances
			// through cleanup or registration. This release need not join the
			// queue guard while that normal wake marker remains pinned.
			if n.Bits == 0 || n.Bits&(wokenBit|starvingBit) == wokenBit {
				queueComplete(&n, i, c)
			} else {
				p.Pair = n.Published
				p.PC = qUnlockTake
			}
		case qUnlockInspect, qProactiveWake:
			// Release mode remains pinned. A registration whose Add saw an
			// owner leaves the wake to that owner's subsequent Unlock. If
			// Add saw no owner, registration itself must advance the queue.
			a = atomicAction
			if g.PC == qProactiveWake && g.Release&heldBit != 0 {
				finishWake()
				break
			}
			p.Old = n.Bits
			mode := g.Release&starvingBit != 0
			if v == reReadMode {
				mode = p.Old&starvingBit != 0
			}
			q := &n.Queues[g.Pair]
			if mode {
				if p.Old&starvingBit == 0 || q.Hand != 0 || p.Old/waiterUnit == 0 || (p.Old&heldBit != 0 && v != reReadMode) {
					finishWake()
				} else {
					p.Target, p.SendMode, p.PC = q.Order[0], 2, qDequeue
				}
			} else if p.Old/waiterUnit == 0 || p.Old&queueWakeMask(v) != 0 {
				finishWake()
			} else {
				p.New, p.PC = -waiterUnit+wokenBit, qWakeAdd
			}
		case qWakeAdd:
			a = atomicAction
			n.Bits += g.New
			p.Target, p.SendMode, p.PC = n.Queues[g.Pair].Order[0], 1, qDequeue
		case qDequeue:
			if g.Target == 0 {
				out = append(out, queueEdge{Next: n, Actor: uint8(i), Error: "P3: waiter count without queue head"})
				continue
			}
			n.remove(g.Pair, g.Target)
			n.G[g.Target-1].Mode = g.SendMode
			if g.SendMode == 2 {
				n.Queues[g.Pair].Hand = g.Target
				cover = qCoverHand
			}
			p.PC = qSend
		case qSend:
			a = sendAction
			if n.G[g.Target-1].Signal {
				out = append(out, queueEdge{Next: n, Actor: uint8(i), Error: "P2: targeted signal buffer full"})
				continue
			}
			if v != lostWake {
				n.G[g.Target-1].Signal = true
			}
			finishWake()
		}
		add(n, a, note, cover)
	}
	return out
}

func queueWakeMask(v variant) int16 {
	if v == suppressWakeHeld {
		return heldBit | wokenBit | starvingBit
	}
	// A normal wake grants competition, never ownership. Unlike Go's
	// unlockSlow held-bit optimization, keep the wake obligation after
	// barging: the old waiter must run to observe its elapsed wait. See
	// evidence/queue-iteration-1.json for the otherwise fair parked cycle.
	return wokenBit | starvingBit
}

// A self-waking registrar may temporarily execute sender phases after the
// critical-phase enum value while its own operation is still waiting. Phase
// ordering therefore cannot identify threshold eligibility.
func queueAgeEnabled(g queueActor) bool {
	return g.Started && !g.Aged && !g.Owns && !g.ReturnError && g.PC != qDone
}
