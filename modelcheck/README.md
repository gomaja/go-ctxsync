# Lock model gates

This standard-library command checks the Mutex queue protocol and the
writer-preferred RWMutex protocol. It also preserves the original channel-based
sendLock model and its counterexamples. Full searches run explicitly through
the CLI; default Go tests use regressions, limited configurations and graph
oracles.

## Run

The searches are CPU- and memory-heavy: the Mutex gate holds about 4.5 GB.
On a shared machine, pin them to quiet CPUs with `taskset`.

From the repository root, enter the checker module:

```sh
cd modelcheck
mkdir -p ../.scratch
go run . -design queue -limit 16000000 \
  -output ../.scratch/queue-gate.json
go run . -design rw -limit 16000000 \
  -output ../.scratch/rw-gate.json
```

The default design is `queue`; `rw` is a separate gate. `-scenario bounded`
selects finite workloads and terminal reuse. `-scenario liveness` selects
continuous traffic, pending dead-end checks and fair-cycle searches. `-config`
filters bounded configuration names by substring. A successful queue all/liveness
invocation also requires the exhaustive old sendLock search to find its starvation cycle;
its negative-control result is written to the output path plus `.sendlock.json`.

Exit **0** means the selected checks completed with their required outcomes.
Exit **1** means a property counterexample, or failure to detect a required
negative control. Exit **2** means an invalid invocation or an inconclusive
search, including a state limit. A limit never counts as an exhaustive pass.
The state limit applies separately to each configuration.

## Candidate protocols

`queue.go` models a CAS-published lazy queue, its short internal guard, intrusive
waiter order, per-waiter buffered signals, ownership flags and cancellation.
First registration joins the tail; a lost normal wake rejoins the head.
Starvation handoff reserves ownership for the selected head until it accepts,
including when cancellation has occurred. A cancelled recipient releases or
passes ownership rather than dropping it.

The policy follows Go 1.26.9 `internal/sync/mutex.go`, `lockSlow:144-169`, for
head requeue and the recipient's starvation exit test. It deliberately omits
the normal held-bit wake suppression in `unlockSlow:215`. That guard allowed
two bargers to prevent an aged parked waiter from ever receiving a wake. A
normal wake is competitive, so sending it while a barger holds does not grant
concurrent ownership. The failed candidate is preserved in the iteration
evidence and the `suppress-wake-while-held` control.

The current protocol also separates guarded accounting from fast ownership
changes. Count, woken and starvation changes belong to the queue guard; the
held bit can change outside it. One guarded acquisition attempt either takes
ownership or registers with an atomic Add. Registration that observes an
unlocked state initiates a wake before parking. Cancellation and wake accounting
use bounded guarded updates. No CAS retry loop is allowed to retain the guard
indefinitely while other actors toggle the held bit. Atomic steps and guarded
queue operations remain distinct model transitions.

A fresh slow caller first checks context, then makes one Load/CAS probe before
entering the guard. The probe excludes held or starving states and preserves
all other snapshot fields. A busy load or CAS loss always enters the existing
registration path. Its three phases and whole-call liveness obligations are
explicit in [iteration 6](evidence/model-v6-probe-review.md). An unbounded
probe-retry control admits a fair pre-registration starvation cycle.

After validating ownership, a normal release whose pinned Add result already
contains Woken skips the queue guard. The designated recipient must acquire,
requeue or cancel and advance the queue. The release-shortcut graphs check
those newly available orders; see
[iteration 4](evidence/model-v4-review.md). The
`cancel-woken-without-advance` control strands a following waiter and fails P3.

`rw.go` composes the writer Mutex with guarded reader counts and reservations,
an announced writer, queued readers and writer drain notification. An announced
writer excludes new readers while existing readers finish. Cancellation clears
the announcement, reserves and signals the blocked reader batch, and releases
the writer Mutex. A granted reader cancelled before returning removes exactly
one reservation. Reserving a batch before writer-Mutex release prevents a
succeeding writer from overtaking readers whose signals are not yet consumed.

## Properties and finite scope

Mutex bounded schedules cover 2x2, 3x1, 3x2 and 4x1 actors/operations, each with
plain, context, mixed and TryLock-containing assignments, plus head requeue and
head-cancellation/handoff assignments. These are fixed representative method
assignments, with every modeled interleaving explored; they do not enumerate
every possible assignment of methods. RW schedules mix readers, writers,
context operations and try operations, including cancelled announced writers.

P1 excludes simultaneous owners; RW additionally excludes writer/reader
overlap and incorrect reader reservations. P2 rejects blocking protocol
signals. Bounded P3 uses reverse reachability without new cancellation:
every reachable state must have a completion route for the finite workload.
This detects sinks and stranded waiters, but is existential completion rather
than starvation freedom under continuing traffic.

P4 requires terminal normalization and explores fresh acquisitions from idle
residues. Queue publication is retained; waiter nodes, signals, handoffs and
guards must be settled. Fresh probes are finite reuse checks, rather than an
induction over arbitrary lock lifetimes. Coverage records exercise important
transitions without replacing a proof of those transitions' properties.

States are exact full structures, not probabilistic hashes. The searches use
neither random scheduling, actor symmetry reduction, partial-order reduction
nor sampling. Normalization forgets dead locals; the guard also shifts ticket
counters to relative values while preserving equality and order. The preserved
old channel model renumbers queue ranks while preserving their order.

## Continuous traffic and fairness

Liveness workloads retain an uncancelled victim while other actors begin new
operations after releases. The search stops expanding paths once that victim
has acquired: those paths already satisfy that pending call's obligation.
An SCC search identifies recurrent sets in which the same victim stays pending
without ownership. It prunes unfair enabling vertices and recomputes SCCs,
rather than rejecting an entire component that may contain a fair subset.

Strong actor fairness means that an actor with progress enabled infinitely
often must take progress steps infinitely often. Closing a context's Done
channel and ageing a waiter are environmental events, not actor progress.
An actor parked without a signal or cancellation has no enabled progress step;
fairness cannot itself wake that actor. Whole-call checking must also exclude
cycles before queue registration, where a slow caller can be prevented from
getting far enough to start its waiter clock.

The one-millisecond threshold is abstracted nondeterministically only after
an operation has started waiting. Age is monotone within that pending call.
Weak threshold justice requires a continuously enabled age process eventually
to execute. Fresh calls pass through states disabling their previous age
process, so they can repeatedly finish before one millisecond. An old pending
victim cannot remain young indefinitely. This is a timing overapproximation;
a timing-sensitive counterexample still needs a feasible wall-clock schedule.

The queue and RW graphs abstract short guard acquisitions and releases. The
implementation discharges the guard progress assumption with FIFO atomic
tickets, rather than assuming Go's `sync.Mutex` is fair. The product packs a
32-bit next counter and 32-bit serving counter into one atomic Uint64. Lock
Adds to next and checks serving until its ticket reaches the head, yielding
with Gosched while waiting. TryLock checks the complete idle word and CASes
that same snapshot with next incremented, so counter wrap cannot turn a stale
partial snapshot into a successful acquisition. Unlock CASes a serving-only
increment without carrying into next. The independent guard model checks these
atomic steps, counter wrap and its unsafe partial-snapshot negative control
separately from the ownership queue policy. The counter protocol requires
fewer than 2^32 outstanding claims.

Under strong actor fairness and bounded guarded bodies, an earlier owner must
finish and advance serving; once a ticket reaches the head, later tickets
cannot advance past it until that owner unlocks. This is the refinement argument
for using the abstract guard in the larger graphs. Unlock's CAS retries are
finite: while this head is held, each competing caller can claim at most one
ticket and then waits, and TryLock cannot claim behind it. This argument is
conditional on the outstanding-claim bound. Guard progress is conditional on the
stated scheduler assumption, not a general Go runtime fairness guarantee.
Scheduler safe points, GC, integer overflow, pool reuse and implementation
memory ordering beyond the modeled atomics and signals are outside the queue
and RW graphs. The Go memory model's
[atomic](https://go.dev/ref/mem#atomic),
[channel](https://go.dev/ref/mem#chan) and
[lock](https://go.dev/ref/mem#locks) rules supply ordering, not scheduling
fairness. A finite fair-cycle result is scoped to its workload, abstraction
and assumptions; it is not an unconditional runtime liveness theorem or a
performance measurement.

## Preserve the old sendLock control

```sh
go run . -design sendlock \
  -scenario starvation -output ../.scratch/sendlock-witness.json
go run . -design sendlock \
  -scenario liveness -limit 16000000 -output ../.scratch/sendlock-exhaustive.json
go run . -design sendlock \
  -scenario bounded -limit 16000000 -output ../.scratch/sendlock-bounded.json
```

The first two deliberately exit 1 when they find starvation. The directed
witness reaches an exact repeated state at transitions 45 and 79: all three
actors run during the 34-transition cycle, the uncancelled context victim never
owns, and each fresh plain waiter can finish below the threshold. The exhaustive
SCC search can report a different reachable prefix with an equivalent cycle.

The old failure is queue order: a woken old waiter loses to a barger, rejoins
behind a fresh waiter, and enters starvation. The fresh head receives handoff
and leaves starvation because its own wait was short. Go's `queueLifo`
counterpart preserves the old waiter's position. See
[the source account](../docs/sendlock-starvation.md).

The old model in `mutex.go` translates the sendLock snapshot recorded in
[sources.json](evidence/sources.json). It includes proposed lazy publication,
entry context checking and atomic starvation-aware TryLock extensions;
the source sendLock eagerly initializes its channels. Each atomic Load/CAS/Add,
allocation, publication, select registration/commitment, receive, send and
cancellation is explicit. Two buffered channels and abandoned registration
credits are retained. FIFO parked select delivery follows Go's `runtime/chan.go`
`waitq.dequeue` and `runtime/select.go` `selectgo`: one committed arm prevents
later cancellation from winning that same select. Stale losing queue entries
are collapsed after commitment because they cannot win again.

## Negative controls and mutation tests

Queue controls include tail requeue, starvation exit ignoring the recipient's
age, cancellation dropping handed ownership, a cancelled node retained in the
queue, a lost wake, overwritten lazy publication, rereading release mode,
TryLock ignoring starvation, and suppressing normal wake while held. Policy
mutations have named tests as well as searches where applicable:

```sh
go test . \
  -run '^TestQueueLostWakeRequeuesAtHead$' -count=1 \
  -queue-model-variant=requeue-tail
go run . -design queue \
  -scenario bounded -config 2x2/plain -variant omit-wake-send
go run . -design rw \
  -scenario bounded -variant writer-cancel-forgets-readers
```

RW also controls reader-count undo errors and readers admitted past an announced
writer. Historical sendLock controls remain explicitly selectable:

```sh
go run . -design sendlock \
  -scenario bounded -config 3x2/plain -variant reread-release-mode
go run . -design sendlock \
  -scenario exit-normalization -variant leave-stale-woken
go run . -design sendlock \
  -scenario bounded -config 2x2/plain -variant omit-wake-send
go run . -design sendlock \
  -scenario bounded -config 3x1/plain -variant overwrite-lazy-publication
go run . -design sendlock \
  -scenario bounded -config 2x2/try -variant try-ignores-starvation
```

The historical stale-woken regression starts from the seeded state in go-sctp's
`TestSendLockStarvationExit`; it enforces that test's idle-cleanup invariant,
rather than claiming a zero-start failure of the weaker fresh-acquisition P4
condition. Its counterpart is not accepted as a no-op queue variant. Model
mutation tests exercise modeled transitions; they are distinct from mutations
of actual package source.

## Evidence and provenance

The [evidence index](evidence/README.md) lists the retained final gates, old
sendLock negative control, broken variants and correctness reviews. Historical
records retain their original design, variant, configuration, exhaustiveness
and state/edge counts. The checker source and tests here replay the current
protocol; earlier graph records describe the candidate named in each file.
Source SHA-256 records identify upstream snapshots by repository, relative path
and Go version. Search elapsed times are host observations, not lock benchmark
results. Runtime tests and benchmark evidence are separate from these graphs.

This module has no external dependencies. It and `benchmarks/` have separate
`go.mod` files, excluding their sources and evidence from the consumer module
zip. Repository automation remains in the consumer module zip.

Run the independent guard gates from this directory:

```sh
go test . -run '^TestTicketGuard' -count=1 -v
```

The guard tests cover all 44 bounded/continuous configurations, packed Try
across wrap and the separate-counter negative. `CTXSYNC_GUARD_EVIDENCE` can
name an output file for the 40-transition separate-counter witness. Runtime
source and continuous-traffic producer mutations run from the repository root
with `python3 scripts/mutate.py`; all listed mutations must be killed.
The controls include nil-Done head requeue, fractional TryLock allocation and
queue/RW guard retention at pending dead ends.

Pooled allocation assertions run without race instrumentation: Go deliberately
discards `sync.Pool` entries under `-race`. Uncontended allocation assertions
and concurrency regressions remain enabled in race runs.

## License

Apache-2.0, with Go-derived portions retaining the BSD 3-Clause terms.
[LICENSE](LICENSE), [GO_LICENSE](GO_LICENSE) and [NOTICE](NOTICE) are included
here so a separate module download retains the applicable terms and attribution.
