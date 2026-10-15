#!/usr/bin/env python3
# Copyright 2026 gomaja. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Run one protocol mutation at a time in disposable checkout-local copies."""

import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / ".scratch/runtime-mutations"

MUTATIONS = [
    ("requeue-tail", "TestMutexStarvationFreedomAfterLostWake", "mutex.go",
     "q.insert(node, !start.IsZero())", "q.insert(node, false)"),
    ("nil-done-requeue-tail", "TestMutexStarvationFreedomAfterLostWake", "mutex.go",
     "q.insert(node, !start.IsZero())", "q.insert(node, !start.IsZero() && ctx.Done() != nil)"),
    ("fractional-try-allocation", "TestMutexUncontendedAllocations", "mutex.go",
     "func (m *Mutex) TryLock() bool {",
     "var allocationCounter atomic.Uint64\nvar allocationSink atomic.Pointer[byte]\n\nfunc (m *Mutex) TryLock() bool {\n\tif allocationCounter.Add(1)%4 == 0 { allocationSink.Store(new(byte)) }"),
    ("exit-ignores-age", "TestMutexStarvationFreedomAfterLostWake", "mutex.go",
     "time.Since(start) <= starvationThreshold || old/mutexWaiter == 1", "true"),
    ("cancel-drops-handoff", "TestMutexCancelledCommittedHandoffReleasesOwnership", "mutex.go",
     "if err != nil {\n\t\t\t\tm.Unlock()\n\t\t\t\treturn err", "if err != nil {\n\t\t\t\treturn err"),
    ("cancel-leaves-node", "TestMutexCancellationRemovesEveryQueuePosition", "mutex.go",
     "if node.mode == waiterQueued {\n\t\t\t\tq.remove(node)\n\t\t\t}", "if node.mode == waiterQueued {}"),
    ("lost-wake", "TestMutexStarvationFreedomAfterLostWake", "mutex.go",
     "node.mode = waiterWoken\n\tnode.wake <- struct{}{}", "node.mode = waiterWoken"),
    ("lazy-publication-race", "TestMutexConcurrentInitializationPublishesOneQueue", "mutex.go",
     "if m.queue.CompareAndSwap(nil, q) {", "m.queue.Store(q)\n\tif true {"),
    ("leave-stale-woken", "TestMutexCancelledCommittedWakeNormalizes", "mutex.go",
     "if node.mode == waiterWoken {\n\t\t\t\tdelta -= mutexWoken\n\t\t\t}", "if node.mode == waiterWoken {}"),
    ("handoff-exit-not-normalized", "TestMutexCancelledCommittedHandoffReleasesOwnership", "mutex.go",
     "if time.Since(start) <= starvationThreshold || old/mutexWaiter == 1 {\n\t\t\t\tdelta -= mutexStarving\n\t\t\t}",
     "if time.Since(start) <= starvationThreshold || old/mutexWaiter == 1 {}"),
    ("try-steals-handoff", "TestMutexTryRefusesReservedHandoff", "mutex.go",
     "if old&(mutexLocked|mutexStarving) != 0 {", "if old&mutexLocked != 0 {"),
    ("guarded-cas-retry", "TestMutexSlowCASCannotHoldQueueGuardForever", "mutex.go",
     "newState := m.state.Add(delta)\n\t\tq.insert", "var newState int32\n\t\tfor {\n\t\t\told = m.state.Load()\n\t\t\tif testHookMutexBeforeCAS != nil { testHookMutexBeforeCAS(m, old) }\n\t\t\tif m.state.CompareAndSwap(old, old+delta) { newState = old+delta; break }\n\t\t}\n\t\tq.insert"),
    ("pool-retains-token", "TestWaiterReuseClearsTokenAndLinks", "mutex.go",
     "case <-w.wake:", "case <-make(chan struct{}):"),
    ("pool-retains-links", "TestWaiterReuseClearsTokenAndLinks", "mutex.go",
     "w.prev, w.next, w.mode = nil, nil, 0", "w.mode = 0"),
    ("guard-try-stale-head", "TestTicketGuardTryRejectsStaleFreeSnapshot", "guard.go",
     "return g.word.CompareAndSwap(old, old+guardTailUnit)", "current := g.word.Load()\n\tif uint32(current>>32) != uint32(old>>32) { return false }\n\treturn g.word.CompareAndSwap(current, current+guardTailUnit)"),
    ("guard-head-wrap-carry", "TestTicketGuardReleaseNotifiesNextClaim", "guard.go",
     "old&0xffffffff00000000|uint64(head)", "old + 1"),
    ("writer-cancel-forgets-readers", "TestRWCancelledWriterReleasesReaders", "rwmutex.go",
     "rw.pending = false\n\t\trw.grantReadersLocked()", "rw.pending = false"),
    ("reader-count-undo-error", "TestRWCancelledGrantedReaderUndoesCount", "rwmutex.go",
     "if node.mode == readerGranted {\n\t\t\trw.releaseReaderLocked()", "if node.mode == readerGranted {"),
    ("reader-past-pending-writer", "TestRWPendingWriterBlocksFreshReaders", "rwmutex.go",
     "if !rw.pending {", "if !rw.writer {"),
    ("reader-batch-without-reservations", "TestRWReleasedReaderBatchPrecedesNextWriter", "rwmutex.go",
     "rw.removeReaderLocked(node)\n\t\trw.readers++", "rw.removeReaderLocked(node)"),
    ("ownerless-registration-lost-wake", "TestMutexOwnerlessRegistrationWakes", "mutex.go",
     "if newState&mutexLocked == 0 {", "if false {"),
    ("cancel-normal-recipient-does-not-advance", "TestMutexCancelledWakeAdvancesFollower", "mutex.go",
     "handed := m.wakeLocked(q, newState)", "handed := false\n\t\t\t_ = newState"),
    ("release-skip-evades-misuse-panic", "TestMutexIllegalUnlockOfReservedHandoffPanics", "mutex.go",
     "if (released+mutexLocked)&mutexLocked == 0 {", "if false {"),
    ("guard-poll-budget-never-yields", "TestTicketGuardPollingYieldsAtBudget", "guard.go",
     "polls++\n\t\tif polls < guardPollBudget {", "polls = 0\n\t\tif polls < guardPollBudget {"),
    ("guard-poll-reuses-stale-head", "TestTicketGuardPollingYieldsAtBudget", "guard.go",
     "for polls := 0; ; {\n\t\thead := uint32(g.word.Load())", "head := uint32(g.word.Load())\n\tfor polls := 0; ; {"),
    ("guard-poll-skips-gosched", "TestTicketGuardYieldsLetOthersRun", "guard.go",
     "testHookGuardYield(g, ticket, false, polls)\n\t\t}\n\t\truntime.Gosched()", "testHookGuardYield(g, ticket, false, polls)\n\t\t}\n\t\t_ = runtime.Gosched"),
    ("guard-slot-skips-gosched", "TestTicketGuardYieldsLetOthersRun", "guard.go",
     "testHookGuardYield(g, ticket, true, polls)\n\t\t}\n\t\truntime.Gosched()", "testHookGuardYield(g, ticket, true, polls)\n\t\t}\n\t\t_ = runtime.Gosched"),
    ("guard-later-claims-poll-head", "TestTicketGuardFarClaimWaitsForItsSlot", "guard.go",
     "for polls := 1; slot.Load() == seen; polls++ {", "_ = seen\n\tfor polls := 1; uint32(g.word.Load()) != ticket; polls++ {"),
    ("guard-slot-omits-head-recheck", "TestTicketGuardNotificationBeforeSlotRead", "guard.go",
     "if ticket-uint32(g.word.Load()) <= 1 {\n\t\treturn\n\t}", ""),
    ("guard-release-omits-notify", "TestTicketGuardReleaseNotifiesNextClaim", "guard.go",
     "g.waitSlot(head + 1).Add(1)", "_ = g.waitSlot(head + 1)"),
    ("guard-release-notifies-wrong-slot", "TestTicketGuardReleaseNotifiesNextClaim", "guard.go",
     "g.waitSlot(head + 1).Add(1)", "g.waitSlot(head + 2).Add(1)"),
    ("guard-release-notifies-unallocated-claim", "TestTicketGuardReleaseNotifiesNextClaim", "guard.go",
     "if uint32(old>>32)-head > 1 {", "if uint32(old>>32)-head > 0 {"),
    ("guard-slot-budget-never-yields", "TestTicketGuardLaterClaimsWaitOnSlots", "guard.go",
     "for polls := 1; slot.Load() == seen; polls++ {", "for polls := 1; slot.Load() == seen; {"),
    ("fresh-probe-ignores-starvation", "TestMutexFreshProbeRefusesReservedHandoff", "mutex.go",
     "if probe&(mutexLocked|mutexStarving) == 0 {", "if probe&mutexLocked == 0 {"),
    ("fresh-probe-drops-accounting", "TestMutexFreshProbeAcquiresWithoutGuardAndPreservesFlags", "mutex.go",
     "m.state.CompareAndSwap(probe, probe|mutexLocked)", "m.state.CompareAndSwap(probe, mutexLocked)"),
    ("fresh-probe-retries-before-registration", "TestMutexFreshProbeLossRegisters", "mutex.go",
     "probe := m.state.Load()\n\tif probe&(mutexLocked|mutexStarving) == 0 {\n\t\tif testHookMutexProbeBeforeCAS != nil {\n\t\t\ttestHookMutexProbeBeforeCAS(m, probe)\n\t\t}\n\t\tif m.state.CompareAndSwap(probe, probe|mutexLocked) {\n\t\t\treturn nil\n\t\t}\n\t}",
     "for {\n\t\tif err := ctx.Err(); err != nil { return err }\n\t\tprobe := m.state.Load()\n\t\tif probe&(mutexLocked|mutexStarving) == 0 && m.state.CompareAndSwap(probe, probe|mutexLocked) { return nil }\n\t}"),
    # Recreate the historical unsafe transition: rereading alone is masked
    # by the new held-state defense, so the control also removes that defense.
    ("historical-release-reread-hands-bargers-ownership", "TestMutexDelayedNormalReleaseCannotHandOffBargersOwnership", "mutex.go",
     "if released&mutexStarving != 0 {\n\t\tif old&mutexStarving == 0 || old&mutexLocked != 0 || old/mutexWaiter == 0 || q.hand != nil {",
     "if old&mutexStarving != 0 {\n\t\tif old&mutexStarving == 0 || old/mutexWaiter == 0 || q.hand != nil {"),
    ("historical-starvation-exit-leaves-stale-woken", "TestMutexCancelledCommittedHandoffReleasesOwnership", "mutex.go",
     "delta -= mutexStarving\n\t\t\t}\n\t\t\tm.state.Add(delta)",
     "delta -= mutexStarving - mutexWoken\n\t\t\t}\n\t\t\tm.state.Add(delta)"),
    ("pending-handoff-retains-guard", "TestRequiredTargetedQueueLivenessGate", "modelcheck/queue.go",
     "\t\t\tfinishWake()\n\t\t}\n\t\tadd(n, a, note, cover)",
     "\t\t\tfinishWake()\n\t\t\tretainGuard := g.SendMode == 2 && g.ResumeWait\n\t\t\tfor j := 0; j < c.Actors; j++ {\n\t\t\t\tif j != i && s.G[j].PC != qWait && s.G[j].PC != qTake { retainGuard = false }\n\t\t\t}\n\t\t\tif retainGuard { n.Queues[g.Pair].Guard = uint8(i + 1) }\n\t\t}\n\t\tadd(n, a, note, cover)"),
    ("pending-rw-release-retains-guard", "TestRequiredRWContinuousTrafficGate/reader/plain", "modelcheck/rw.go",
     "case rwGuardRelease:\n\t\t\tn.Guard = 0",
     "case rwGuardRelease:\n\t\t\tif i != 1 || g.Next != rwMutexRelease { n.Guard = 0 }"),
]


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    baseline = subprocess.run(["taskset", "-c", "8-23", "go", "test", ".", "-count=1"],
                              cwd=ROOT, capture_output=True, text=True)
    (OUTPUT / "baseline.log").write_text(baseline.stdout + baseline.stderr)
    if baseline.returncode:
        raise SystemExit("baseline failed")
    rows = []
    for name, test, filename, old, new in MUTATIONS:
        target = OUTPUT / name
        target.mkdir(exist_ok=True)
        module = ROOT / "modelcheck" if filename.startswith("modelcheck/") else ROOT
        for source in [module / "go.mod", *module.glob("*.go")]:
            shutil.copy2(source, target / source.name)
        source = target / Path(filename).name
        original = source.read_text()
        if original.count(old) != 1:
            raise SystemExit(f"{name}: expected one mutation site, got {original.count(old)}")
        source.write_text(original.replace(old, new, 1))
        command = ["taskset", "-c", "8-23", "go", "test", ".", "-v", "-run", f"^{test}$", "-count=1", "-timeout", "10s"]
        result = subprocess.run(command, cwd=target, capture_output=True, text=True)
        log = OUTPUT / f"{name}.log"
        log.write_text(result.stdout + result.stderr)
        output = result.stdout + result.stderr
        killed = result.returncode != 0 and ("--- FAIL:" in output or "panic:" in output)
        expected = {
            "nil-done-requeue-tail": "aged waiter lost priority",
            "fractional-try-allocation": "total allocations",
            "guard-poll-budget-never-yields": "next claim did not yield at its budget or did not observe the released head",
            "guard-poll-reuses-stale-head": "next claim did not yield at its budget or did not observe the released head",
            "guard-try-stale-head": "a stale free snapshot acquired a future ticket before the current head",
            "guard-head-wrap-carry": "guard not idle after three releases",
            "guard-poll-skips-gosched": "intervals between its yields",
            "guard-slot-skips-gosched": "intervals between its yields",
            "guard-later-claims-poll-head": "timed out waiting for ticket 2 to keep watching its slot",
            "guard-slot-omits-head-recheck": "guard claims did not finish",
            "guard-release-omits-notify": "first release changed slots",
            "guard-release-notifies-wrong-slot": "first release changed slots",
            "guard-release-notifies-unallocated-claim": "release notified a claim that does not exist",
            "guard-slot-budget-never-yields": "timed out waiting for yields while the guard is held",
            "pending-handoff-retains-guard": "pending queue victim at a dead end",
            "pending-rw-release-retains-guard": "pending RW victim at a dead end",
        }.get(name)
        if expected is not None:
            killed = killed and expected in output
        rows.append(dict(mutation=name, test=test, command=command, exit=result.returncode, killed=killed,
                         evidence=str(log.relative_to(ROOT))))
        (OUTPUT / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
        print(name, result.returncode, flush=True)
    (OUTPUT / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
    if any(not row["killed"] for row in rows):
        raise SystemExit("one or more mutations survived or failed before running its test")


if __name__ == "__main__":
    main()
