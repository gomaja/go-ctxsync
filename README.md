# go-ctxsync

`ctxsync` provides cancellable `Mutex` and `RWMutex` types for Go 1.26. Plain
lockers and context lockers share the same queue policy. The package uses only
the standard library and starts no goroutines.

```sh
go get github.com/gomaja/go-ctxsync@main
```

The project uses the main branch and publishes no version tags.

## Use

The zero value is ready to use. Protect the same data with the same lock for
every access, and release it only after a successful acquisition.

```go
var mu ctxsync.Mutex
counts := map[string]int{}

if err := mu.LockContext(ctx); err != nil {
    return err
}
counts["item"]++
mu.Unlock()
```

`RWMutex` adds shared read locking and cancellable `RLockContext`. Read lockers
must only read the protected data. Both types must not be copied after first
use. Like the standard library's locks, they are not bound to a goroutine.

| Type | Methods |
| --- | --- |
| `Mutex` | `Lock()`, `TryLock() bool`, `LockContext(context.Context) error`, `Unlock()` |
| `RWMutex` | `Lock()`, `TryLock() bool`, `LockContext(context.Context) error`, `Unlock()`, `RLock()`, `TryRLock() bool`, `RLockContext(context.Context) error`, `RUnlock()` |

## Context and synchronization contracts

`LockContext` and `RLockContext` return nil only while the caller holds the
requested lock. Otherwise they return `ctx.Err()` and retain no ownership. An
already-done context fails before acquiring. A nil context panics; a context
with a nil `Done()` channel behaves like the corresponding plain operation.
When cancellation races acquisition, either result is possible. A cancelled
handoff recipient releases or passes ownership before returning an error.

Mutex Unlock synchronizes before a later successful Lock, LockContext or
TryLock. RWMutex writer Unlock synchronizes before subsequent successful read
or writer acquisitions; a reader's RUnlock synchronizes before the next
successful writer acquisition. Successful context and try methods have the
corresponding ordinary method's ordering. Failed try methods and context calls
returning an error establish no such guarantee. These are the lock boundaries
described by the [Go memory model](https://go.dev/ref/mem#locks).

Unlocking an unlocked Mutex, using RWMutex.Unlock without a writer, or using
RUnlock without an active reader panics. These are recoverable panics, unlike
the standard library locks' fatal errors. If a misuse panic is recovered, the
lock's state is undefined; discard the lock. Recursive read locking can deadlock
behind a pending writer. Read-to-write upgrades and write-to-read downgrades
are unsupported.

## Queue policy

Mutex follows Go 1.26's normal and starvation modes. Normal mode permits new
callers to barge ahead of a woken waiter. A waiter that loses that race rejoins
at the head, preserving its priority over newer registrations. Once an old
waiter is still denied ownership after waiting more than one millisecond, it
sets starvation mode. Ownership then passes directly to the queue head.
Starvation ends when the receiving waiter is the last one, or its own wait
has not exceeded the threshold. Context waiters and plain waiters use the
same intrusive queue and per-waiter wake objects.

Two details strengthen progress relative to a direct Go mutex adaptation.
Normal unlock retains its wake obligation even if a barger has already taken
the lock. A normal wake invites competition, rather than granting ownership,
so this preserves normal barging while allowing the old waiter to observe its
age. Go's `internal/sync/mutex.go:215` suppresses that wake when the held bit
is set; copying that optimization admitted a fair parked-waiter cycle in the
model. The implementation therefore retains Go's mode and queue policy while
deliberately changing this wake scheduling optimization.

The queue guard protects waiter counts, wake flags and starvation flags. Only
the ownership bit changes outside it. A fresh slow caller makes one Load/CAS
probe before entering the guard. It can barge only outside starvation mode
and preserves every other state field. A busy snapshot or CAS loss proceeds
to the guard without retrying. Guarded acquisition makes one attempt to
take an available lock; a loss registers the waiter with an atomic Add under
the guard. Registration can set starvation even if that Add observes the lock
as unlocked, because a waiter is already accounted for. In that case it also
initiates the appropriate wake before parking. Cleanup and normal wake
accounting use bounded guarded updates. Mutex's guarded accounting has no CAS
retry loop, so an endlessly changing ownership bit cannot keep that accounting
section occupied through retries. The uncontended Lock fast path is one CAS;
the uncontended Unlock fast path is one Add. Neither touches the queue.

A release whose pinned snapshot already contains a normal wake can return
without entering the guard. Its recipient supplies the next acquisition,
requeue or cancellation cleanup; ownerless registration still initiates a wake.
Starvation releases retain their handoff obligation. The
[release-shortcut gate](modelcheck/evidence/model-v4-review.md)
checks the additional interleavings, including cancellation of that recipient.

RWMutex serializes writers through a Mutex with the same policy. A separate
short guard protects active readers, an announced writer, queued readers and
the current writer's drain waiter. After announcing, a writer waits for the
existing readers and blocks new readers. Writer cancellation clears that
announcement, releases every reader it held back, and releases the writer
Mutex. A reader batch is counted as active before releasing the writer Mutex,
so the next writer must wait even for readers that have not consumed their
signals yet. Cancellation after a reader grant removes exactly one such
reservation. Read paths have an internal guard cost in addition to reader
accounting.

Both short accounting guards use FIFO tickets, with next and serving counters
packed into one atomic Uint64. An Add to the upper next counter assigns a ticket.
Only the next claim polls the packed word, yielding with Gosched after at most
64 mismatching loads. Later claims watch per-claim slots in a static 4096-entry
array shared by all guards. Each reads its slot before rechecking the head,
then polls that slot, yielding after at most 64 loads. The release that makes
an allocated claim next in line increments its slot. A changed slot prompts a
fresh head check; hash collisions only cause spurious rechecks. Slots are
64-bit counters, so a claim misses its notification only if its slot is
incremented a multiple of 2^64 times while it is not looking. This follows
Dice and Kogan's [TWA - Ticket Locks Augmented with a Waiting Array](https://arxiv.org/abs/1810.01573v4)
(Euro-Par 2019).

In Go 1.26.9's race runtime, an atomic Load of an address with synchronization
state read-locks that state, while Add and CompareAndSwap write-lock it; new
readers are admitted while a writer waits. When every waiter polled the packed
word, release and ticket allocation stalled for seconds
([issue #1](https://github.com/gomaja/go-ctxsync/issues/1)). Moving later claims
to slots limits that contention on the packed word. See the
[guard review](modelcheck/evidence/model-v7-guard-review.md) for the runtime
evidence and the model's scope.

TryLock CASes the complete idle word, preserving its snapshot across counter
wrap. Unlock CASes the serving increment without carrying into next. While
the head remains held, each competing caller can claim only one ticket before
waiting, so those CAS retries are finite under the protocol's bound of fewer
than 2^32 outstanding claims. The guard creates no goroutines and allocates
nothing; the waiting array is static storage.

With strongly fair actor scheduling, every earlier ticket eventually advances:
its owner has a bounded accounting body, and later tickets cannot pass its
serving position. Reading the slot before rechecking the head prevents a
release notification from being lost. This supplies the guard progress
argument without assuming the fairness of Go's `sync.Mutex`. FIFO applies only
to these short internal sections; user lock ownership still follows normal
barging and starvation handoff. Waiting costs atomic loads and scheduler
yields, which the contention benchmarks include.

The one-millisecond threshold controls the queue policy. It is not a maximum
acquisition latency. Progress also depends on lock holders releasing their
locks and runnable goroutines receiving execution time. The finite checker
uses explicit actor and threshold fairness assumptions; it does not prove
unconditional liveness for arbitrary Go runtime executions.

## Allocation

Uncontended Mutex fast paths allocate nothing. A zero-value mutex creates its
queue lazily on contention and publishes it with CAS. Racing publishers can
each allocate a candidate; only the winning queue remains installed.

Waiter nodes and their buffered wake channels are pooled. The first use,
higher peak concurrency, or a refill after garbage collection empties the
pool can allocate. With enough cached nodes, the waiting and cancellation
paths reuse them. Allocation-free context waiting also requires context methods
to need no initialization or allocation, including an already-created Done
channel. RWMutex uses the same writer queue and pooled reader and
drain nodes. The package does not allocate a goroutine per waiter.

## Validation and benchmarks

Run package tests, race tests and the explicit model gates separately:

```sh
go test ./... -count=1
go test -race ./... -count=1
(cd modelcheck && go run . -design queue -limit 16000000)
(cd modelcheck && go run . -design rw -limit 16000000)
```

The checker and its evidence live in a separate module, so consumers do not
download them with the package. The checker examines bounded safety and reuse
properties, then searches continuous-traffic graphs for pending dead ends and
fair starvation cycles. Its old channel-based sendLock model remains a negative
control with an exact 34-transition cycle.
See [the checker documentation](modelcheck/README.md) for scope,
assumptions, mutations and exit codes, and
[the sendLock account](docs/sendlock-starvation.md) for source provenance.

Benchmarks live in a [separate module](benchmarks/README.md), keeping x/sync
out of the package dependency graph. They compare standard-library locks,
weighted semaphores and a TryLock retry design. Runs use CPUs 8–23, ten
samples and GOMAXPROCS 4 and 16 for contention; direct Mutex measurements also
compare the uncontended primitive paths. RW batches have exact 90/10 or 10/90
read/write mixes. Benchmark output and benchstat, rather than a design claim,
report the measured ns/op throughput costs.

Measured on Linux/amd64, Go 1.26.9, AMD Ryzen AI Max+ 395, CPUs 8–23.
Each value is the median of ten one-second samples in ns/op. Mutex operations
count one acquire/release pair; RW rows count **ten** pairs. All ctxsync
samples report zero allocations. Standard-library context columns measure
entry-check overhead and cannot cancel a blocked acquisition.

| Operation | Workers | sync | ctxsync | semaphore | retry |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mutex direct pair | 1 | 8.26 | 8.41 | — | — |
| Mutex plain pair | 1 | 8.80 | 9.04 | 19.23 | 9.54 |
| Mutex try pair | 1 | 8.87 | 8.88 | 18.04 | 9.37 |
| Mutex background context | 1 | 9.03 | 9.21 | 18.75 | 10.20 |
| Mutex open cancellable context | 1 | 9.75 | 10.04 | 21.21 | 12.43 |
| Mutex plain pair | 4 | 18.23 | 15.21 | 351.25 | 21.37 |
| Mutex plain pair | 16 | 27.19 | 15.50 | 291.20 | 30.31 |
| Mutex background context | 4 | 21.30 | 16.36 | 397.90 | 21.63 |
| Mutex background context | 16 | 26.97 | 18.79 | 353.55 | 30.65 |
| Mutex open cancellable context | 4 | 18.21 | 18.63 | 444.60 | 22.88 |
| Mutex open cancellable context | 16 | 27.86 | 19.58 | 350.40 | 25.84 |
| RW 90% reads, plain | 4 | 125.05 | 233.00 | 2137.00 | 133.40 |
| RW 90% reads, plain | 16 | 130.10 | 245.45 | 5604.50 | 139.55 |
| RW 90% writes, plain | 4 | 277.45 | 343.05 | 2710.00 | 287.55 |
| RW 90% writes, plain | 16 | 343.35 | 348.35 | 2715.50 | 351.85 |

The direct Mutex pair differs by 0.15 ns from sync. Plain Mutex contention
is faster than sync at both worker counts. RW paths pay for internal
guarded reader accounting. The measured read-mostly RW cases take about
84–108% longer than sync.RWMutex; those costs are shown separately. RW retry values
use the `retry-serialized` comparator. See [complete raw results and benchstat](benchmarks/results/2026-10-14-final)
for every context/RW case and statistical comparisons. Earlier failed candidates
remain in adjacent result directories.

The table predates the waiting-array guard. Interleaved runs against its
predecessor found no significant change in any Mutex case, while contended RW
cases took up to about 9% longer in the quietest run, mostly in read-mostly
mixes. Under the race detector, cancelling 128 queued callers dropped from
18–38 seconds to milliseconds. See the
[waiting-array measurements](benchmarks/results/2026-10-15-v7-waiting-array).

## License

Apache-2.0, with Go-derived portions retaining the BSD 3-Clause terms in
[GO_LICENSE](GO_LICENSE). See [NOTICE](NOTICE) for source attribution.
