// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Go-derived reader/writer policy is covered by GO_LICENSE.
// Copyright 2009 The Go Authors. All rights reserved.

package main

import "fmt"

const (
	rwForgetReaders variant = "writer-cancel-forgets-readers"
	rwUndoReader    variant = "reader-count-undo-error"
	rwAdmitReader   variant = "reader-past-pending-writer"
)

type rwKind uint8

const (
	rwReader rwKind = iota
	rwWriter
	rwContextReader
	rwContextWriter
	rwTryReader
	rwTryWriter
)

func (k rwKind) writer() bool  { return k&1 != 0 }
func (k rwKind) context() bool { return k == rwContextReader || k == rwContextWriter }
func (k rwKind) try() bool     { return k == rwTryReader || k == rwTryWriter }

type rwPhase uint8

const (
	rwEntry rwPhase = iota
	rwReadTake
	rwReadInspect
	rwReadWait
	rwReadReleaseTake
	rwReadRelease
	rwDrainSignal
	rwMutexAcquire
	rwWriteTake
	rwWriteInspect
	rwWriteWait
	rwWriteReleaseTake
	rwWriteRelease
	rwBatchGrant
	rwBatchSignal
	rwGuardRelease
	rwMutexRelease
	rwCritical
	rwDone
)

var rwPhaseNames = [...]string{"entry", "reader-guard", "reader-inspect", "reader-wait", "reader-release-guard", "reader-release", "drain-send", "writer-mutex", "writer-guard", "writer-inspect", "writer-drain-wait", "writer-release-guard", "writer-release", "reader-grant", "reader-send", "guard-release", "writer-mutex-release", "critical", "done"}

func (p rwPhase) String() string { return rwPhaseNames[p] }

type rwActor struct {
	PC, Next                                                                rwPhase
	Op, Target                                                              uint8
	Owns, Writer, Cancelled, Started, Aged, Signal, Granted, Drain, Success bool
}

// Readers counts both active reader owners and granted reservations. Reserving
// each queued reader before releasing the writer Mutex makes the next writer
// wait for that batch, even before its members have consumed their signals.
type rwState struct {
	W                       queueState
	G                       [4]rwActor
	Readers, Pending, Guard uint8
	Queue                   [4]uint8
}

type rwConfig struct {
	Name        string
	Actors, Ops int
	Kinds       [4][2]rwKind
	Repeat      [4]bool
}

type rwEdge struct {
	Next        rwState
	Actor       uint8
	Action      action
	Note, Error string
	Cover       uint64
}

const (
	rwCoverReaderQueue uint64 = 1 << iota
	rwCoverReaderGrant
	rwCoverCancelReader
	rwCoverCancelGranted
	rwCoverWriterDrain
	rwCoverCancelWriter
	rwCoverWriterAcquire
	rwCoverDrainSignal
)

var rwCoverage = [...]string{"reader queue", "reader batch grant", "cancel queued reader", "cancel granted reader", "writer drain wait", "cancel announced writer", "writer acquisition", "last reader drain signal"}

func rwInitial(c rwConfig) rwState {
	s := rwState{}
	for i := range s.G {
		s.W.G[i].PC = qDone
		if i >= c.Actors {
			s.G[i].PC = rwDone
		}
	}
	return s
}

func (s *rwState) removeReader(id uint8) bool {
	for i, entry := range s.Queue {
		if entry == id {
			copy(s.Queue[i:], s.Queue[i+1:])
			s.Queue[len(s.Queue)-1] = 0
			return true
		}
	}
	return false
}

func (s *rwState) queueReader(id uint8) {
	for i, entry := range s.Queue {
		if entry == 0 {
			s.Queue[i] = id
			return
		}
	}
	panic("RW model reader queue overflow")
}

func rwComplete(s *rwState, i int, c rwConfig) {
	op := s.G[i].Op + 1
	s.G[i] = rwActor{Op: op}
	s.W.G[i] = queueActor{PC: qDone}
	if c.Repeat[i] {
		s.G[i] = rwActor{}
	} else if int(op) >= c.Ops {
		s.G[i] = rwActor{PC: rwDone}
	}
}

func rwNormalize(s rwState) rwState {
	for i := range s.G {
		g := &s.G[i]
		if g.PC != rwGuardRelease {
			g.Next = 0
		}
		if g.PC != rwBatchSignal && g.PC != rwDrainSignal {
			g.Target = 0
		}
		if g.Success {
			g.Started, g.Aged, g.Cancelled = false, false, false
		}
	}
	return s
}

func rwInvariant(s rwState) string {
	if p := queueInvariant(s.W); p != "" {
		return "writer Mutex " + p
	}
	readers, writers := 0, 0
	for i, g := range s.G {
		if g.Owns {
			if g.Writer {
				writers++
				if !s.W.G[i].Owns || s.Pending != uint8(i+1) {
					return "P1: writer owns without writer serialization or announcement"
				}
			} else {
				readers++
			}
		}
		if g.Granted {
			readers++
			if g.Writer || g.Owns {
				return "P1: invalid reader reservation"
			}
		}
	}
	if writers > 1 || writers != 0 && readers != 0 {
		return "P1: writer overlaps reader or writer"
	}
	if readers != int(s.Readers) {
		return "P1: reader count differs from owners and reservations"
	}
	if s.Pending != 0 && !s.W.G[s.Pending-1].Owns {
		return "P1: writer announcement lacks serialization ownership"
	}
	var queued uint8
	for _, id := range s.Queue {
		if id == 0 {
			continue
		}
		if queued&(1<<(id-1)) != 0 {
			return "P1: duplicate queued reader"
		}
		queued |= 1 << (id - 1)
		g := s.G[id-1]
		if g.PC == rwDone || g.Granted || g.Owns || g.Writer {
			return "P3: invalid or retained reader queue node"
		}
	}
	return ""
}

func rwIdle(s rwState) bool {
	if s.Readers != 0 || s.Pending != 0 || s.Guard != 0 || s.Queue != [4]uint8{} || !queueIdle(s.W) {
		return false
	}
	for _, g := range s.G {
		if g.Signal || g.Drain || g.Granted || g.Owns {
			return false
		}
	}
	return true
}

func rwTerminal(s rwState, c rwConfig) bool {
	for i := 0; i < c.Actors; i++ {
		if s.G[i].PC != rwDone {
			return false
		}
	}
	return true
}

func rwWriterConfig(s rwState, c rwConfig) config {
	wc := config{Actors: c.Actors, Ops: 1}
	for i := 0; i < c.Actors; i++ {
		k := c.Kinds[i][s.G[i].Op]
		switch {
		case k.try():
			wc.Kinds[i][0] = tryLock
		case k.context():
			wc.Kinds[i][0] = contextLock
		}
	}
	return wc
}

func rwSuccessors(s rwState, c rwConfig, v variant) []rwEdge {
	out := make([]rwEdge, 0, c.Actors*3)
	wc := rwWriterConfig(s, c)
	mutexEdges := queueSuccessors(s.W, wc, correct)
	for i := 0; i < c.Actors; i++ {
		g := s.G[i]
		if g.PC == rwDone {
			continue
		}
		k := c.Kinds[i][g.Op]
		id := uint8(i + 1)
		add := func(n rwState, a action, note string, cover uint64) {
			out = append(out, rwEdge{Next: rwNormalize(n), Actor: uint8(i), Action: a, Note: note, Cover: cover})
		}
		if k.context() && !g.Cancelled && !g.Success {
			n := s
			n.G[i].Cancelled = true
			if g.PC == rwMutexAcquire {
				n.W.G[i].Cancelled = true
			}
			add(n, cancelAction, "close Done", 0)
		}
		if g.Started && !g.Aged && !g.Success {
			n := s
			n.G[i].Aged = true
			if queueAgeEnabled(n.W.G[i]) {
				n.W.G[i].Aged = true
			}
			add(n, ageAction, "continuous wait crosses 1 ms", 0)
		}
		if g.PC == rwMutexAcquire || g.PC == rwMutexRelease {
			for _, e := range mutexEdges {
				if int(e.Actor) != i || e.Action == cancelAction || e.Action == ageAction {
					continue
				}
				n := s
				n.W = e.Next
				n.G[i].Started = g.Started || n.W.G[i].Started
				if n.W.G[i].PC == qCritical && g.PC == rwMutexAcquire {
					n.G[i].PC = rwWriteTake
				} else if n.W.G[i].PC == qDone {
					rwComplete(&n, i, c)
				}
				if e.Error != "" {
					out = append(out, rwEdge{Next: n, Actor: uint8(i), Error: e.Error})
				} else {
					add(n, e.Action, "writer Mutex: "+e.Note, 0)
				}
			}
			continue
		}
		n := s
		p := &n.G[i]
		a, note, cover := localAction, "RW "+g.PC.String(), uint64(0)
		release := func(next rwPhase) { p.PC, p.Next = rwGuardRelease, next }
		finishWriter := func() {
			n.Pending = 0
			p.Drain = false
			if g.Cancelled && v == rwForgetReaders {
				release(rwMutexRelease)
			} else {
				p.PC = rwBatchGrant
			}
		}
		switch g.PC {
		case rwEntry:
			if g.Cancelled {
				rwComplete(&n, i, c)
			} else if k.writer() {
				p.Writer, p.PC = true, rwMutexAcquire
				n.W.G[i] = queueActor{}
			} else {
				p.PC = rwReadTake
			}
		case rwReadTake, rwReadReleaseTake, rwWriteTake, rwWriteReleaseTake:
			if n.Guard != 0 {
				if k.try() && !g.Success {
					if g.Writer {
						p.PC = rwMutexRelease
						n.W.G[i].PC = qUnlockAdd
					} else {
						rwComplete(&n, i, c)
					}
				} else {
					continue
				}
			} else {
				n.Guard = id
				switch g.PC {
				case rwReadTake:
					p.PC = rwReadInspect
				case rwReadReleaseTake:
					p.PC = rwReadRelease
				case rwWriteTake:
					p.PC = rwWriteInspect
				case rwWriteReleaseTake:
					p.PC = rwWriteRelease
				}
			}
		case rwReadInspect:
			if g.Cancelled {
				if g.Granted {
					if v != rwUndoReader {
						n.Readers--
					}
					p.Granted = false
					cover = rwCoverCancelGranted
				} else if n.removeReader(id) {
					cover = rwCoverCancelReader
				}
				p.Signal = false
				if g.Granted && n.Readers == 0 && n.Pending != 0 {
					p.Target, p.PC, p.Next = n.Pending, rwDrainSignal, rwDone
				} else {
					release(rwDone)
				}
			} else if g.Granted {
				p.Granted, p.Owns, p.Success = false, true, true
				release(rwCritical)
			} else if n.Pending == 0 || v == rwAdmitReader {
				if n.Pending != 0 {
					out = append(out, rwEdge{Next: n, Actor: uint8(i), Error: "P3: fresh reader admitted past pending writer"})
					continue
				}
				n.Readers++
				p.Owns, p.Success = true, true
				release(rwCritical)
			} else if k.try() {
				release(rwDone)
			} else {
				n.queueReader(id)
				p.Started = true
				release(rwReadWait)
				cover = rwCoverReaderQueue
			}
		case rwReadWait:
			if !g.Signal && !g.Cancelled {
				continue
			}
			p.Signal = false
			p.PC = rwReadTake
			a = selectAction
		case rwReadRelease:
			n.Readers--
			p.Owns = false
			if n.Readers == 0 && n.Pending != 0 {
				p.Target, p.PC, p.Next = n.Pending, rwDrainSignal, rwDone
			} else {
				release(rwDone)
			}
		case rwDrainSignal:
			if n.G[g.Target-1].Drain {
				out = append(out, rwEdge{Next: n, Actor: uint8(i), Error: "P2: writer drain signal buffer full"})
				continue
			}
			n.G[g.Target-1].Drain = true
			release(rwDone)
			a, cover = sendAction, rwCoverDrainSignal
		case rwWriteInspect:
			if g.Cancelled {
				if n.Pending == id {
					cover = rwCoverCancelWriter
					finishWriter()
				} else {
					release(rwMutexRelease)
				}
			} else if k.try() && n.Readers != 0 {
				release(rwMutexRelease)
			} else {
				n.Pending = id
				if n.Readers == 0 {
					p.Owns, p.Success = true, true
					p.Drain = false
					release(rwCritical)
					cover = rwCoverWriterAcquire
				} else {
					p.Started = true
					release(rwWriteWait)
					cover = rwCoverWriterDrain
				}
			}
		case rwWriteWait:
			if !g.Drain && !g.Cancelled {
				continue
			}
			p.Drain = false
			p.PC = rwWriteTake
			a = selectAction
		case rwCritical:
			if g.Writer {
				p.PC = rwWriteReleaseTake
			} else {
				p.PC = rwReadReleaseTake
			}
		case rwWriteRelease:
			p.Owns = false
			finishWriter()
		case rwBatchGrant:
			if n.Queue[0] == 0 {
				release(rwMutexRelease)
			} else {
				p.Target = n.Queue[0]
				n.removeReader(p.Target)
				n.G[p.Target-1].Granted = true
				n.Readers++
				p.PC = rwBatchSignal
				cover = rwCoverReaderGrant
			}
		case rwBatchSignal:
			if n.G[g.Target-1].Signal {
				out = append(out, rwEdge{Next: n, Actor: uint8(i), Error: "P2: reader signal buffer full"})
				continue
			}
			n.G[g.Target-1].Signal = true
			p.PC = rwBatchGrant
			a = sendAction
		case rwGuardRelease:
			n.Guard = 0
			p.PC = g.Next
			switch g.Next {
			case rwDone:
				rwComplete(&n, i, c)
			case rwMutexRelease:
				n.W.G[i].PC = qUnlockAdd
			}
		default:
			panic(fmt.Sprintf("unknown RW phase %d", g.PC))
		}
		add(n, a, note, cover)
	}
	return out
}

func rwConfigurations() []rwConfig {
	var out []rwConfig
	for _, size := range [][2]int{{2, 2}, {3, 1}, {3, 2}, {4, 1}} {
		for mode, name := range []string{"plain", "context", "mixed", "try"} {
			c := rwConfig{Name: fmt.Sprintf("%dx%d/%s", size[0], size[1], name), Actors: size[0], Ops: size[1]}
			for i := 0; i < c.Actors; i++ {
				for op := 0; op < c.Ops; op++ {
					k := rwKind((i + op) % 2)
					if mode == 1 || mode == 2 && (i+op)%3 == 0 {
						k += 2
					} else if mode == 3 && (i+op)%3 == 0 {
						k += 4
					}
					c.Kinds[i][op] = k
				}
			}
			out = append(out, c)
		}
	}
	return out
}
