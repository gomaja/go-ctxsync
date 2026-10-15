// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitGuardCondition(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// guardClaims starts claims on g and joins them in cleanup. Cleanup first
// calls release so failed tests do not leave claims behind; a hook is cleared
// only after every claim has returned.
type guardClaims struct {
	t    *testing.T
	g    *ticketGuard
	done chan struct{}
	n    int
}

func newGuardClaims(t *testing.T, g *ticketGuard, release func(), clear func()) *guardClaims {
	c := &guardClaims{t: t, g: g, done: make(chan struct{}, 16)}
	t.Cleanup(func() {
		release()
		timeout := time.After(5 * time.Second)
		for ; c.n > 0; c.n-- {
			select {
			case <-c.done:
			case <-timeout:
				t.Error("guard claims did not finish; hooks left installed")
				return
			}
		}
		clear()
	})
	return c
}

// start begins one claim and waits until it has allocated its ticket.
func (c *guardClaims) start(body func()) {
	c.t.Helper()
	want := uint32(c.g.word.Load()>>32) + 1
	c.n++
	go func() { c.g.Lock(); body(); c.g.Unlock(); c.done <- struct{}{} }()
	waitGuardCondition(c.t, "ticket allocation", func() bool { return uint32(c.g.word.Load()>>32) == want })
}

func (c *guardClaims) join() {
	c.t.Helper()
	timeout := time.After(5 * time.Second)
	for ; c.n > 0; c.n-- {
		select {
		case <-c.done:
		case <-timeout:
			c.t.Fatal("guard claims did not finish")
		}
	}
}

func TestTicketGuardPollingYieldsAtBudget(t *testing.T) {
	var g ticketGuard
	g.Lock()
	var release sync.Once
	var mu sync.Mutex
	rounds, onSlot := 0, true
	testHookGuardYield = func(gg *ticketGuard, _ uint32, slot bool, polls int) {
		if gg == &g {
			mu.Lock()
			rounds, onSlot = polls, slot
			mu.Unlock()
			release.Do(g.Unlock)
		}
	}
	claims := newGuardClaims(t, &g, func() { release.Do(g.Unlock) }, func() { testHookGuardYield = nil })
	acquired := make(chan struct{}, 1)
	claims.start(func() { acquired <- struct{}{} })
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("next claim did not yield at its budget or did not observe the released head")
	}
	claims.join()
	mu.Lock()
	defer mu.Unlock()
	if rounds != guardPollBudget || onSlot {
		t.Fatalf("next claim yielded after %d polls, watching its slot: %t", rounds, onSlot)
	}
}

// Only the claim next in line polls the packed word. Later claims watch their
// slots, so the word's synchronization is not kept busy by every waiting
// claim under the race detector (issue #1).
func TestTicketGuardLaterClaimsWaitOnSlots(t *testing.T) {
	var g ticketGuard
	g.Lock()
	var release sync.Once
	var mu sync.Mutex
	recording := true
	type yield struct {
		slot  bool
		polls int
	}
	yields := map[uint32][]yield{}
	testHookGuardYield = func(gg *ticketGuard, ticket uint32, slot bool, polls int) {
		if gg != &g {
			return
		}
		mu.Lock()
		if recording {
			yields[ticket] = append(yields[ticket], yield{slot, polls})
		}
		mu.Unlock()
	}
	claims := newGuardClaims(t, &g, func() { release.Do(g.Unlock) }, func() { testHookGuardYield = nil })
	var order []uint32
	for ticket := uint32(1); ticket <= 3; ticket++ {
		claims.start(func() { order = append(order, ticket) })
	}
	waitGuardCondition(t, "yields while the guard is held", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(yields[1]) >= 3 && len(yields[2]) >= 3 && len(yields[3]) >= 3
	})
	mu.Lock()
	recording = false
	mu.Unlock()
	release.Do(g.Unlock)
	claims.join()
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("claims entered in order %v", order)
	}
	for ticket, ys := range yields {
		for _, y := range ys {
			if y.slot != (ticket > 1) || y.polls != guardPollBudget {
				t.Fatalf("claim %d yielded %+v; only claims behind the next one watch slots, %d loads per yield", ticket, ys, guardPollBudget)
			}
		}
	}
}

// A claim watching its slot does not read the head again until the slot
// changes, even after the head reaches its ticket.
func TestTicketGuardFarClaimWaitsForItsSlot(t *testing.T) {
	var g ticketGuard
	g.Lock()
	g.word.Add(guardTailUnit) // ticket 1 never polls
	var mu sync.Mutex
	slotYields := 0
	testHookGuardYield = func(gg *ticketGuard, ticket uint32, slot bool, _ int) {
		if gg == &g && ticket == 2 && slot {
			mu.Lock()
			slotYields++
			mu.Unlock()
		}
	}
	yieldsSoFar := func() int { mu.Lock(); defer mu.Unlock(); return slotYields }
	var entered atomic.Bool
	notify := func() {
		g.word.Store(3<<32 | 2)
		g.waitSlot(2).Add(1)
	}
	var cleanup sync.Once
	claims := newGuardClaims(t, &g, func() { cleanup.Do(notify) }, func() { testHookGuardYield = nil })
	claims.start(func() { entered.Store(true) })
	waitGuardCondition(t, "ticket 2 to watch its slot", func() bool { return yieldsSoFar() >= 1 })
	g.word.Store(3<<32 | 2) // head reaches ticket 2 without a release notifying its slot
	n := yieldsSoFar()
	waitGuardCondition(t, "ticket 2 to keep watching its slot", func() bool { return yieldsSoFar() >= n+3 })
	if entered.Load() {
		t.Fatal("a claim watching its slot entered without the slot changing")
	}
	cleanup.Do(func() { g.waitSlot(2).Add(1) })
	claims.join()
	if !entered.Load() || !g.TryLock() {
		t.Fatal("the notified claim did not enter and release the guard")
	}
	g.Unlock()
}

// Unlock increments exactly the slot of the claim its release makes next in
// line, and only when that claim exists, also across counter wrap.
func TestTicketGuardReleaseNotifiesNextClaim(t *testing.T) {
	for _, start := range []uint32{0, math.MaxUint32 - 1, math.MaxUint32} {
		var g ticketGuard
		g.word.Store(uint64(start)<<32 | uint64(start))
		g.Lock()                  // ticket start
		g.word.Add(guardTailUnit) // start+1 and start+2 never poll
		g.word.Add(guardTailUnit)
		slots := func() [4]uint64 {
			var v [4]uint64
			for i := range v {
				v[i] = g.waitSlot(start + uint32(i)).Load()
			}
			return v
		}
		before := slots()
		g.Unlock() // start+1 enters; start+2 becomes next in line
		after := slots()
		if after != [4]uint64{before[0], before[1], before[2] + 1, before[3]} {
			t.Fatalf("start %d: first release changed slots %v to %v", start, before, after)
		}
		g.Unlock() // start+2 enters; no claim behind it exists
		if again := slots(); again != after {
			t.Fatalf("start %d: release notified a claim that does not exist: %v to %v", start, after, again)
		}
		g.Unlock()
		if !g.TryLock() {
			t.Fatalf("start %d: guard not idle after three releases", start)
		}
		g.Unlock()
	}
}

// The release that makes a claim next in line can complete after the claim
// decides to wait but before it reads its slot. Rechecking the head after
// reading the slot keeps that notification from being lost.
func TestTicketGuardNotificationBeforeSlotRead(t *testing.T) {
	var g ticketGuard
	g.Lock()
	var release sync.Once
	testHookGuardSlot = func(gg *ticketGuard, ticket uint32) {
		if gg == &g && ticket == 2 {
			release.Do(g.Unlock)
		}
	}
	claims := newGuardClaims(t, &g, func() { release.Do(g.Unlock) }, func() { testHookGuardSlot = nil })
	claims.start(func() {})
	claims.start(func() {})
	claims.join()
	if !g.TryLock() {
		t.Fatal("guard not idle after both claims")
	}
	g.Unlock()
}

// Waiting claims must actually yield, including with one P: another goroutine
// keeps running between their yields. The race detector shuffles run queues
// (Go 1.26.9 runtime/proc.go, randomizeScheduler), so the check counts yield
// intervals in which the observer advanced rather than requiring every one.
func TestTicketGuardYieldsLetOthersRun(t *testing.T) {
	const intervals = 16
	old := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(old) })
	var g ticketGuard
	g.Lock()
	var observed atomic.Uint64
	stop := make(chan struct{})
	observer := make(chan struct{})
	go func() {
		defer close(observer)
		for {
			select {
			case <-stop:
				return
			default:
			}
			observed.Add(1)
			runtime.Gosched()
		}
	}()
	type progress struct {
		last                       uint64
		yields, advanced, slotSeen int
	}
	var mu sync.Mutex
	recording := true
	claims := map[uint32]*progress{1: {}, 2: {}}
	testHookGuardYield = func(gg *ticketGuard, ticket uint32, slot bool, _ int) {
		if gg != &g {
			return
		}
		v := observed.Load()
		mu.Lock()
		defer mu.Unlock()
		p := claims[ticket]
		if !recording || p == nil {
			return
		}
		if p.yields > 0 && v != p.last {
			p.advanced++
		}
		if slot {
			p.slotSeen++
		}
		p.last = v
		p.yields++
	}
	var release sync.Once
	waiting := newGuardClaims(t, &g, func() { release.Do(g.Unlock) }, func() {
		testHookGuardYield = nil
		close(stop)
		<-observer
	})
	waiting.start(func() {})
	waiting.start(func() {})
	waitGuardCondition(t, "yields from both waiting claims", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return claims[1].yields > intervals && claims[2].yields > intervals
	})
	mu.Lock()
	recording = false
	mu.Unlock()
	release.Do(g.Unlock)
	waiting.join()
	for ticket, p := range claims {
		if p.advanced*4 < p.yields-1 {
			t.Fatalf("claim %d: other goroutines ran in %d of %d intervals between its yields", ticket, p.advanced, p.yields-1)
		}
		want := 0
		if ticket == 2 {
			want = p.yields
		}
		if p.slotSeen != want {
			t.Fatalf("claim %d: %d of %d yields watched its slot", ticket, p.slotSeen, p.yields)
		}
	}
}

func TestTicketGuardExclusion(t *testing.T) {
	var g ticketGuard
	var wg sync.WaitGroup
	value := 0
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			for j := 0; j < 500; j++ {
				g.Lock()
				value++
				g.Unlock()
			}
		})
	}
	wg.Wait()
	if value != 2000 || !g.TryLock() {
		t.Fatal("guard exclusion or idle reuse")
	}
	g.Unlock()
}

func TestTicketGuardWrapKeepsTail(t *testing.T) {
	var g ticketGuard
	g.word.Store(uint64(math.MaxUint32)<<32 | uint64(math.MaxUint32))
	g.Lock()
	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() { g.Lock(); close(acquired); g.Unlock(); close(done) }()
	g.Unlock()
	<-acquired
	<-done
	if !g.TryLock() {
		t.Fatal("head wrap carried into the tail and left a phantom ticket")
	}
	g.Unlock()
}

func TestTicketGuardTryRejectsStaleFreeSnapshot(t *testing.T) {
	var g ticketGuard
	testHookGuardTry = func(gg *ticketGuard) {
		if gg == &g {
			gg.word.Store(uint64(math.MaxUint32))
		}
	}
	defer func() { testHookGuardTry = nil }()
	if g.TryLock() {
		t.Fatal("a stale free snapshot acquired a future ticket before the current head")
	}
}
