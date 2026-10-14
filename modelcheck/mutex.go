// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Models the sendLock in go-sctp/send_linux.go and Go's mutex mode policy.
// Go-derived portions are covered by GO_LICENSE.
// Copyright 2024 The Go Authors. All rights reserved.

package main

import "fmt"

const (
	heldBit int16 = 1 << iota
	wokenBit
	starvingBit
	waiterUnit
)

type kind uint8

const (
	plainLock kind = iota
	contextLock
	tryLock
)

type variant string

const (
	correct          variant = "correct"
	reReadMode       variant = "reread-release-mode"
	staleWoken       variant = "leave-stale-woken"
	lostWake         variant = "omit-wake-send"
	lazyPublish      variant = "overwrite-lazy-publication"
	tryIgnoresStarve variant = "try-ignores-starvation"
)

type phase uint8

const (
	entry phase = iota
	fastCAS
	tryLoad
	tryCAS
	initLoad
	initAllocate
	initPublish
	initReload
	slowLoad
	slowCAS
	selectWait
	parked
	wakeTime
	wakeLoad
	wakeCAS
	handTime
	handLoad
	handAdd
	cancelRecord
	critical
	unlockAdd
	unlockCheck
	settleLoad
	settleCAS
	settleCount
	unlockDecide
	unlockReload
	clearFlagsCAS
	claimWakeCAS
	channelLoad
	wakeSend
	handSend
	finished
)

var phaseNames = [...]string{
	"entry", "fast-CAS", "try-load", "try-CAS", "init-load", "init-allocate",
	"init-publish", "init-reload", "slow-load", "slow-CAS", "select", "parked",
	"wake-time", "wake-load", "wake-CAS", "hand-time", "hand-load", "hand-add",
	"cancel-record", "critical", "unlock-add", "unlock-check", "settle-load",
	"settle-CAS", "settle-count", "unlock-decide", "unlock-reload", "clear-flags-CAS",
	"claim-wake-CAS", "channel-load", "wake-send", "hand-send", "finished",
}

func (p phase) String() string { return phaseNames[p] }

// All local values that can survive an interleaving are part of the key.
// Pair names are allocation identities; channel publication is not collapsed
// into initialization. Old and Credit are snapshots used by later atomics.
type actor struct {
	Old            int16
	Credit         int16
	PC             phase
	Op             uint8
	Pair           uint8
	Queue          uint8
	Cancelled      bool
	Started        bool
	Aged           bool
	Starved        bool
	ReleaseStarved bool
	Owns           bool
}

type state struct {
	Bits      int16
	Abandoned int16
	Published uint8
	Wake      [5]bool
	Hand      [5]bool
	G         [4]actor
}

type config struct {
	Name       string
	Actors     int
	Ops        int
	Kinds      [4][2]kind
	Repeat     [4]bool
	IdleAtExit bool
}

type action uint8

const (
	atomicAction action = iota
	cancelAction
	ageAction
	selectAction
	allocateAction
	sendAction
	localAction
)

type edge struct {
	Next   state
	Actor  uint8
	Action action
	Note   string
	Error  string
	Cover  uint64
}

const (
	coverEnterStarvation uint64 = 1 << iota
	coverExitStarvation
	coverHandoff
	coverStaleHand
	coverStaleWake
	coverInitRace
	coverCancelBeforeSelect
	coverCancelParked
	coverCancelCommittedWake
	coverCancelCommittedHand
	coverBarge
	coverSettle
)

var coverageNames = [...]string{
	"starvation entry", "starvation exit", "handoff", "buffered handoff",
	"wake token consumed without woken flag", "lazy publication loser",
	"cancel before select", "cancel parked select", "cancel committed wake",
	"cancel committed handoff", "normal-mode barging", "abandoned credit settlement",
}

func invariant(s state) string {
	owners := 0
	for _, g := range s.G {
		if g.Owns {
			owners++
		}
	}
	if owners > 1 {
		return "P1: multiple owners"
	}
	if s.Bits < 0 || s.Abandoned < 0 {
		return "P1: negative waiter/credit state"
	}
	return ""
}

// send implements runtime/chan.go's queued-receiver commitment. Both arms of
// a parked select are removed atomically when a send wins selectDone. A
// subsequently closed Done channel cannot also choose cancellation. For a
// buffered channel with no receiver, the token occupies its sole slot.
func send(s state, pair uint8, hand bool) (state, bool) {
	oldest := -1
	for i, g := range s.G {
		if g.PC == parked && g.Pair == pair && (oldest < 0 || g.Queue < s.G[oldest].Queue) {
			oldest = i
		}
	}
	if oldest >= 0 {
		g := &s.G[oldest]
		g.PC = wakeTime
		if hand {
			g.PC = handTime
		}
		g.Queue = 0
		return s, false
	}
	if hand {
		if s.Hand[pair] {
			return s, true
		}
		s.Hand[pair] = true
	} else {
		s.Wake[pair] = true // nonblocking wake send: a full buffer selects default
	}
	return s, false
}

func complete(s *state, i int, c config) {
	if c.Repeat[i] {
		s.G[i] = actor{}
		return
	}
	nextOp := s.G[i].Op + 1
	s.G[i] = actor{Op: nextOp}
	if int(nextOp) >= c.Ops {
		s.G[i].PC = finished
	}
}

func terminal(s state, c config) bool {
	for i := 0; i < c.Actors; i++ {
		if s.G[i].PC != finished {
			return false
		}
	}
	return true
}

// normalize forgets only dead local values and renumbers queue positions
// without changing their order. No atomic or channel operation is combined.
func normalize(s state) state {
	for i := range s.G {
		g := &s.G[i]
		if g.PC >= critical && g.PC < finished {
			g.Cancelled, g.Started, g.Aged, g.Starved = false, false, false, false
			if g.PC != wakeSend && g.PC != handSend {
				g.Pair = 0
			}
		}
		switch g.PC {
		case tryCAS, slowCAS, wakeCAS, handAdd, unlockCheck, settleLoad,
			settleCAS, unlockDecide, clearFlagsCAS, claimWakeCAS:
		default:
			g.Old = 0
		}
		if g.PC != settleCAS {
			g.Credit = 0
		}
	}
	var ranks [4]uint8
	for i, g := range s.G {
		if g.Queue > 0 {
			ranks[i] = 1
			for _, other := range s.G {
				if other.Queue > 0 && other.Queue < g.Queue {
					ranks[i]++
				}
			}
		}
	}
	for i := range s.G {
		s.G[i].Queue = ranks[i]
	}
	return s
}

func successors(s state, c config, v variant) []edge {
	out := make([]edge, 0, c.Actors*3)
	for i := 0; i < c.Actors; i++ {
		g := s.G[i]
		if g.PC == finished {
			continue
		}
		k := c.Kinds[i][g.Op]
		add := func(n state, a action, note string, cover uint64) {
			out = append(out, edge{Next: normalize(n), Actor: uint8(i), Action: a, Note: note, Cover: cover})
		}
		if k == contextLock && !g.Cancelled && g.PC < cancelRecord {
			n := s
			n.G[i].Cancelled = true
			cover := coverCancelBeforeSelect
			switch g.PC {
			case parked:
				n.G[i].PC, n.G[i].Queue = cancelRecord, 0
				cover = coverCancelParked
			case wakeTime, wakeLoad, wakeCAS:
				cover = coverCancelCommittedWake
			case handTime, handLoad, handAdd:
				cover = coverCancelCommittedHand
			}
			add(n, cancelAction, "close Done", cover)
		}
		if g.Started && !g.Aged && g.PC < cancelRecord {
			n := s
			n.G[i].Aged = true
			add(n, ageAction, "wait exceeds 1 ms", 0)
		}
		n := s
		p := &n.G[i]
		a := atomicAction
		note := g.PC.String()
		var cover uint64
		switch g.PC {
		case entry:
			a = localAction
			if k == contextLock && g.Cancelled {
				complete(&n, i, c)
			} else if k == tryLock {
				p.PC = tryLoad
			} else {
				p.PC = fastCAS
			}
		case fastCAS:
			if n.Bits == 0 {
				n.Bits = heldBit
				p.PC, p.Owns = critical, true
			} else {
				p.PC = initLoad
			}
		case tryLoad:
			p.Old = n.Bits
			mask := heldBit | starvingBit
			if v == tryIgnoresStarve {
				mask = heldBit
			}
			if p.Old&mask != 0 {
				complete(&n, i, c)
			} else {
				p.PC = tryCAS
			}
		case tryCAS:
			if n.Bits == g.Old {
				n.Bits |= heldBit
				p.PC, p.Owns = critical, true
				if g.Old&starvingBit != 0 {
					out = append(out, edge{Next: normalize(n), Actor: uint8(i), Action: a, Note: note, Error: "fairness: TryLock barged in starvation mode"})
					continue
				}
			} else {
				complete(&n, i, c)
			}
		case initLoad, initReload:
			p.Pair = n.Published
			p.PC = slowLoad
			if p.Pair == 0 {
				p.PC = initAllocate
			}
		case initAllocate:
			a = allocateAction
			p.Pair, p.PC = uint8(i+1), initPublish
		case initPublish:
			if n.Published == 0 || v == lazyPublish {
				n.Published, p.PC = p.Pair, slowLoad
			} else {
				p.PC = initReload
				cover = coverInitRace
			}
		case slowLoad:
			p.Old, p.PC = n.Bits, slowCAS
		case slowCAS:
			if n.Bits != g.Old {
				p.PC = slowLoad
			} else if g.Old&(heldBit|starvingBit) == 0 {
				n.Bits |= heldBit
				p.PC, p.Owns = critical, true
				if g.Old/waiterUnit > 0 {
					cover = coverBarge
				}
			} else {
				n.Bits += waiterUnit
				if g.Starved && g.Old&heldBit != 0 {
					n.Bits |= starvingBit
					if g.Old&starvingBit == 0 {
						cover = coverEnterStarvation
					}
				}
				p.Started, p.PC = true, selectWait
			}
		case selectWait:
			// Select can choose any already-ready arm. Otherwise it
			// atomically queues on wake, hand and Done (runtime/select.go).
			if s.Wake[g.Pair] {
				w := s
				w.Wake[g.Pair], w.G[i].PC = false, wakeTime
				cv := uint64(0)
				if s.Bits&wokenBit == 0 {
					cv = coverStaleWake
				}
				add(w, selectAction, "select buffered wake", cv)
			}
			if s.Hand[g.Pair] {
				h := s
				h.Hand[g.Pair], h.G[i].PC = false, handTime
				add(h, selectAction, "select buffered hand", coverStaleHand)
			}
			if g.Cancelled {
				p.PC = cancelRecord
				add(n, selectAction, "select Done", 0)
			}
			if !s.Wake[g.Pair] && !s.Hand[g.Pair] && !g.Cancelled {
				q := uint8(0)
				for _, other := range s.G {
					if other.Queue > q {
						q = other.Queue
					}
				}
				p.Queue, p.PC = q+1, parked
				add(n, selectAction, "park select", 0)
			}
			continue
		case parked:
			continue // only a send or cancellation can commit this select
		case wakeTime, handTime:
			a = localAction
			p.Starved = g.Starved || g.Aged
			p.PC = wakeLoad
			if g.PC == handTime {
				p.PC = handLoad
			}
		case wakeLoad:
			p.Old, p.PC = n.Bits, wakeCAS
		case wakeCAS:
			if n.Bits == g.Old {
				n.Bits = (g.Old - waiterUnit) &^ wokenBit
				p.Old, p.PC = n.Bits, slowCAS
			} else {
				p.PC = wakeLoad
			}
		case handLoad:
			p.Old, p.PC = n.Bits, handAdd
		case handAdd:
			delta := heldBit - waiterUnit
			if g.Old&starvingBit != 0 && (!g.Starved || g.Old/waiterUnit == 1) {
				delta -= starvingBit
				cover = coverExitStarvation
			}
			n.Bits += delta
			p.PC, p.Owns = critical, true
		case cancelRecord:
			n.Abandoned++
			complete(&n, i, c)
		case critical:
			a, p.PC = localAction, unlockAdd
		case unlockAdd:
			n.Bits -= heldBit
			p.Owns = false
			if n.Bits == 0 {
				complete(&n, i, c)
			} else {
				p.Old, p.PC = n.Bits, unlockCheck
				p.ReleaseStarved = n.Bits&starvingBit != 0
			}
		case unlockCheck:
			a = localAction
			if v == reReadMode {
				p.ReleaseStarved = g.Old&starvingBit != 0
			}
			p.PC = unlockDecide
			if g.Old/waiterUnit != 0 {
				p.PC = settleLoad
			}
		case settleLoad:
			p.Credit = n.Abandoned
			p.PC = settleCAS
			if p.Credit == 0 {
				p.PC = unlockDecide
			}
		case settleCAS:
			if n.Abandoned == g.Credit {
				n.Abandoned--
				p.PC = settleCount
			} else {
				p.PC = settleLoad
			}
		case settleCount:
			n.Bits -= waiterUnit
			p.Old, p.PC = n.Bits, unlockCheck
			cover = coverSettle
		case unlockDecide:
			a = localAction
			if g.ReleaseStarved {
				p.PC = clearFlagsCAS
				if g.Old/waiterUnit != 0 {
					p.PC = channelLoad
				}
			} else if g.Old&(heldBit|starvingBit) != 0 {
				complete(&n, i, c)
			} else if g.Old/waiterUnit == 0 {
				p.PC = clearFlagsCAS
				if g.Old&wokenBit == 0 {
					complete(&n, i, c)
				}
			} else if g.Old&wokenBit != 0 {
				complete(&n, i, c)
			} else {
				p.PC = claimWakeCAS
			}
		case unlockReload:
			p.Old, p.PC = n.Bits, unlockCheck
		case clearFlagsCAS:
			if n.Bits == g.Old {
				mask := wokenBit
				if g.ReleaseStarved {
					mask |= starvingBit
					if v == staleWoken {
						mask = starvingBit
					}
				}
				n.Bits &^= mask
				if (g.Old^n.Bits)&starvingBit != 0 {
					cover = coverExitStarvation
				}
				complete(&n, i, c)
			} else {
				p.PC = unlockReload
			}
		case claimWakeCAS:
			if n.Bits == g.Old {
				n.Bits |= wokenBit
				p.PC = channelLoad
			} else {
				p.PC = unlockReload
			}
		case channelLoad:
			p.Pair = n.Published
			p.PC = wakeSend
			if g.ReleaseStarved {
				p.PC = handSend
			}
		case wakeSend, handSend:
			a = sendAction
			blocked := false
			if g.PC != wakeSend || v != lostWake {
				n, blocked = send(n, g.Pair, g.PC == handSend)
			}
			if blocked {
				out = append(out, edge{Next: normalize(n), Actor: uint8(i), Action: a, Note: note, Error: "P2: handoff send would block"})
				continue
			}
			if g.PC == handSend {
				cover = coverHandoff
			}
			complete(&n, i, c)
		default:
			panic(fmt.Sprintf("unhandled model PC %d", g.PC))
		}
		add(n, a, note, cover)
	}
	return out
}
