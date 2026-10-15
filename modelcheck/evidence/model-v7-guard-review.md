# Iteration 7: waiting-array guard polling

Only the claim next in line polls the packed ticket word, yielding after at
most 64 mismatching loads. A far claim reads its slot, rechecks the head,
then polls the slot until it changes (`../../guard.go:53-100`). Unlock advances
serving with its unchanged packed CAS, then separately increments the slot of
newHead+1 when `tail - newHead > 1` in that successful snapshot
(`../../guard.go:115-131`). Allocation, packed Try and ticket wrap are unchanged.
This is a ticket lock augmented with a waiting array: Dice and Kogan,
["TWA - Ticket Locks Augmented with a Waiting Array", Euro-Par 2019](https://arxiv.org/abs/1810.01573).
Moving distant waiters to a shared array reduces contention on the word that
allocation and release must modify.

Issue #1 reported that cancelling 128 queued `LockContext` callers under
`-race` with GOMAXPROCS=32 took up to 40.5 seconds, with 3 of 10 runs failing
a three-second per-result watchdog, against about 0.12 seconds without it. On 15 CPUs with GOMAXPROCS=32 the issue's reproducer took 17.9 to 37.9
seconds per run on the base commit and 1.13 to 1.17 seconds with this change,
most of it the race runtime's one-second exit delay. Copies of the two guards
that time ticket allocation and release showed the base release CAS at up to
2.7 to 3.1 seconds and ticket Add at up to 0.8 to 2.3 seconds in three runs,
and at most 0.51 ms and 0.024 ms with the waiting array, whose cancellations
all returned within 19 ms. The runs and logs are in
[the race evidence](../../benchmarks/results/2026-10-15-v7-waiting-array/README.md#race-detector-stall-issue-1).
They are runtime measurements, not results of the finite checker.

Go 1.26.9 and 1.27.2 both build the race runtime from LLVM
`51bfeff0e4b0757ff773da6882f4d538996c9b04` (`src/runtime/race/README:9`).
The 64-bit `sync/atomic` Load, Add and CompareAndSwap enter
`__tsan_go_atomic64_load`, `__tsan_go_atomic64_fetch_add` and
`__tsan_go_atomic64_compare_exchange` (`src/runtime/race_amd64.s:207,288,366`),
which use acquire, acq_rel and acq_rel/acquire orders
([`tsan_interface_atomic.cpp:873-875,893-895,941-947`](https://github.com/llvm/llvm-project/blob/51bfeff0e4b0757ff773da6882f4d538996c9b04/compiler-rt/lib/tsan/rtl/tsan_interface_atomic.cpp#L873-L947)).
An acquire `AtomicLoad` of an address that has a sync object read-locks that
object's mutex (lines 224-247, lock at 239); `AtomicRMW` and `AtomicCAS` with a
release order write-lock it (lines 292 and 428).
[`Mutex::ReadLock`](https://github.com/llvm/llvm-project/blob/51bfeff0e4b0757ff773da6882f4d538996c9b04/compiler-rt/lib/sanitizer_common/sanitizer_mutex.h#L253-L288)
admits a reader unless a writer holds the mutex (`sanitizer_mutex.h:258`),
whereas `Mutex::Lock` also waits for readers (lines 166-209, test at 172) and
`ReadUnlock` wakes a waiting writer only when the reader count reaches zero
(lines 290-308). Concurrent pollers therefore keep the read side occupied and
delay the release and ticket allocation that would let them proceed. The cited
files have SHA-256 `89e287d9a3a14ef3a6a8fcb7b7e969f99715c2ac63a2b1516e65cc332dac92fc`
(`tsan_interface_atomic.cpp`) and
`d44379b53a8041644ad47600686de05f9209f4eee3779d17dd7b31413c2cd533`
(`sanitizer_mutex.h`) at that revision. The graph does not model these
internals; it checks the polling policy.

Round 1 used distance-proportional yields: a claim with k earlier claims
ahead yielded k times before a fresh head Load. Review rejected it because the
cached batch kept yielding even after the claim became head. In the recorded
[latency runs](../../benchmarks/results/2026-10-15-v7-waiting-array/README.md#cancellation-and-guard-drain-latency),
that policy took 11.5 to 13.0 ms to drain 512 claims on one P against 1.4 to
1.8 ms for the base and the waiting array, and its saturated native
cancellation median was 6.4 seconds against 3.5 seconds for the base. The
waiting-array protocol replaces that policy and its cached-backoff model and
witness.

The current graph records the head Load's `Ready` and `Ahead` observations
until its separate branch (`../guard.go:160-184`). A matching observation
enters the body. A next-claim mismatch can poll again or yield; a far mismatch
must start the slot protocol. `guardSlotLoad`, `guardRecheck`, `guardSlotPoll`
and `guardSlotYield` are separate transitions (`../guard.go:185-213`).
An unchanged slot can poll again or yield. A changed slot forces a fresh head
Load. The recheck returns to that loop when the claim is now next or head.
The release CAS clears ownership and records its notification target; a
separate transition performs the slot Add (`../guard.go:214-217,239-259`).
Other claimants can acquire and release before that delayed Add. Try's Load
and complete-word CAS remain separate (`../guard.go:260-277`). Go's
[atomic operations](https://pkg.go.dev/sync/atomic#pkg-overview) have a
sequentially consistent order, which supplies the atomic interleavings here.

Slots are indexed by three-bit ticket, with independent eight-bit notification
counters. Normalization subtracts each slot's current value from its saved
observations modulo 256, resets current values to zero, and shifts ticket and
pending-target indices together (`../guard.go:88-131`). This preserves equality
and inequality, including a real arithmetic wrap, rather than collapsing changed
values back to the saved value. Relative ticket normalization is unchanged.
Once a claim has sampled its slot, serving cannot pass its ticket before that
claim acquires. At most three other actors can carry older delayed notifications,
and at most three preceding claims can release before it becomes head. Even
counting all six as notifying the same slot and adding the optional collision
gives at most seven increments before a fresh sample, below 256. The safety
invariant rejects an eighth increment before it can approach a false ABA
(`../guard.go:306-354`); every reachable positive-graph state passes it.

The additional collision family permits one optional environmental increment
of each watched slot per saved sample (`../guard.go:291-302`). An increment of
an unwatched slot can be omitted: its absolute value is forgotten before the
next sample, and no current comparison can observe it. Environmental edges
have actor -1, no actor-progress flag and no fairness obligation
(`../guard.go:428-455`). Collisions can cause repeated spurious head rechecks
across fresh samples. This family deliberately bounds increments within one
sample interval; it does not prove liveness under arbitrary collision traffic
or a native slot-counter wrap between observations. The product's slots are
64-bit counters, as in the TWA paper: a 32-bit counter could be returned to a
waiting claim's saved value by 2^32 colliding increments while that claim is
descheduled, stranding it at the head, and a 64-bit counter needs 2^64.

| Policy / family | Configurations | States | Edges |
| --- | ---: | ---: | ---: |
| Product bounded | 30 | 225,243 | 866,543 |
| Product continuous traffic | 14 | 870,864 | 3,656,726 |
| Product total | 44 | 1,096,107 | 4,523,269 |
| Product with collisions, bounded | 30 | 369,727 | 1,592,023 |
| Product with collisions, continuous traffic | 14 | 2,176,610 | 10,855,880 |
| Product with collisions, total | 44 | 2,546,337 | 12,447,903 |
| Conservative bounded | 30 | 79,007 | 297,235 |
| Conservative continuous traffic | 14 | 184,412 | 772,018 |
| Conservative total | 44 | 263,419 | 1,069,253 |

Every positive row is exhaustive and passes. Bounded families enumerate all
blocking/Try assignments for one through four actors. Continuous families use
a blocking victim and every repeating blocking/Try assignment for two through
four actors (`../guard_test.go:41-109`). Both searches check safety, pending
dead ends and fair starvation cycles (`../guard.go:356-426`). The bounded
search designates actor 0 as victim; all method assignments cover every other
victim by actor permutation. Slot safety requires observers to be waiting
claims, bounded increments, no retained dead observations and ownership released
before notification, in addition to the original ownership and ticket invariants.
The conservative policy omits slots and retains both mismatch successors,
reproducing the historical 263,419 states / 1,069,253 edges exactly. Packed Try
still passes its 125-state / 247-edge wrap graph. Separate-counter Try still
fails after 93 states / 179 edges, with a 40-transition overlap witness
(`../guard_test.go:12-29,387-417`).

The five named controls each fail liveness while passing the safety invariant.
Their complete three-actor graphs and replayed full-state witnesses are in
[guard-v7-slot-controls.json](guard-v7-slot-controls.json). Each witness has a
fair two-transition cycle: the victim, already at serving, alternates slot
polling and Gosched while the slot equals its saved value. The other two
actors have completed. The cycle therefore does not depend on withholding a
runnable notifier (`../guard_test.go:293-384`).

| Control | States | Edges | Prefix | Cycle |
| --- | ---: | ---: | ---: | ---: |
| Notify before release CAS | 1,134 | 3,360 | 19 | 2 |
| Far claim without head recheck | 1,060 | 3,140 | 17 | 2 |
| Head recheck before slot Load | 1,213 | 3,583 | 18 | 2 |
| Notify newHead+2 | 1,163 | 3,485 | 18 | 2 |
| Notify only when tail - newHead > 2 | 951 | 2,841 | 18 | 2 |

Early notification can be consumed before the head advances. Omitting or
reversing the recheck can sample an already-delivered notification and wait
forever for another. The wrong target and stricter threshold leave the actual
next claim unnotified. Named tests validate fairness, replay the prefix and
closed walk, and require the victim to poll an unchanged slot at head.
Focused successor and normalization tests cover delayed notifications across
later releases, both release/slot-read orderings, slot equality through wrap,
CAS snapshot conditions, spurious increments and slot invariants
(`../guard_test.go:440-634`). The bounded-search regression separately requires
rejection of a fair lost-notification cycle (`../guard_test.go:638-644`).
[guard-v7-poll-mutations.json](guard-v7-poll-mutations.json) records 16 temporary
source mutations and their exit-1 assertion failures, including wrong polling,
recheck order, notification target/condition, normalization and disabled controls.

Native head and slot polling each yield after at most 64 mismatching Loads.
The model admits both another poll and a yield, overapproximating those finite
budgets; multiple native polling/yield rounds map to repetitions of those edges.
The local budget counter and finite local steps are stutter. These steps cannot
hide an infinite native execution without atomic transitions, because each
polling iteration loads either the head or the slot. Under strong actor fairness,
finite guarded bodies and finite CAS interference, preceding owners release.
A release making the claim next either precedes its slot sample, in which case
the following head recheck returns, or follows the sample, in which case the
recheck or slot comparison observes progress. If the notification is delayed,
the releaser remains runnable until it sends it. Once the victim reaches serving,
no later claim can pass it. This supplies the conditional progress refinement
for the abstract guards in the queue and RW models. It assumes fewer than
2^32 product claims and the finite checker scope; it is not a Go scheduler
fairness guarantee.

The native tests supply the concrete budget and scheduler checks:
`TestTicketGuardPollingYieldsAtBudget`,
`TestTicketGuardLaterClaimsWaitOnSlots`,
`TestTicketGuardFarClaimWaitsForItsSlot`,
`TestTicketGuardReleaseNotifiesNextClaim`,
`TestTicketGuardNotificationBeforeSlotRead` and
`TestTicketGuardYieldsLetOthersRun` (`../../guard_test.go:75-333`).
`testHookGuardYield(g, ticket, slot, polls)` runs before each Gosched;
`testHookGuardSlot(g, ticket)` runs before the initial slot read
(`../../guard.go:24-32,68-71,81-84,94-97`).
`TestMutexCancellationBurstReturnsPromptly` and
`TestRWReaderCancellationBurstReturnsPromptly` require 128 cancellations to
return within three seconds with GOMAXPROCS at least 32
(`../../cancellation_burst_test.go:18-98`). `../../scripts/mutate.py` lists the native mutations, each killed by a named
test: either Gosched removed, far claims polling the head, the omitted head
recheck, a missing notification, notification of head+2, notification when
tail - newHead > 0 and a slot wait that never yields. All 40 runtime mutations
were killed in the recorded run ([runtime-mutations.json](runtime-mutations.json)).

The finite graph does not model TSan's internal locks, wall-clock latency,
throughput, the address hash or 4096 physical array entries, arbitrary numbers
of colliding guards, arbitrary collision increments between samples, or a full
native 64-bit slot-counter wrap. Three-bit ticket wrap and eight-bit slot arithmetic
are checked within four actors, rather than all native counter values or claim
counts. Queue/RW models and their 16M-state gates were not modified or rerun;
the module's ordinary regression and race suites were run. Performance and
runtime mutation results remain separate evidence.

Replay from `modelcheck/`, confined to CPUs 0-3:

```sh
CTXSYNC_GUARD_TOTALS_EVIDENCE=evidence/guard-v7-totals.json \
CTXSYNC_GUARD_CONTROLS_EVIDENCE=evidence/guard-v7-slot-controls.json \
taskset -c 0-3 go test . -run '^TestTicketGuard' -count=1 -v
taskset -c 0-3 go test -race ./... -count=1
```

The two generated evidence files were regenerated to separate paths and
compared byte-for-byte on a second run. `CTXSYNC_GUARD_EVIDENCE` still emits
the separate-counter overlap witness. Historical iteration-5 records remain
unchanged.
