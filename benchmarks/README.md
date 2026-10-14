# Lock benchmarks

This separate module keeps `golang.org/x/sync` out of the package's dependencies.
It imports the working checkout through `replace github.com/gomaja/go-ctxsync => ..`.
The comparator dependency is pinned to `golang.org/x/sync v0.24.0`, resolved by
`go list -m -json golang.org/x/sync@latest` on 2026-10-14 and checked against the
upstream tag. The tag commit is `36f2d70ecde9857dc4e363242a1369c7645d6770`.
See the [Weighted API](https://pkg.go.dev/golang.org/x/sync@v0.24.0/semaphore)
and its `semaphore.go` source for the acquisition and cancellation contract.

The suite compares ctxsync, the standard library, a weighted semaphore, and a
benchmark-only TryLock retry design. Mutex semaphore capacity is one. RW
semaphore capacity equals GOMAXPROCS, with readers acquiring one token and
writers acquiring all tokens. RunParallel uses one worker per P, so the capacity
does not constrain simultaneous readers in these workloads.

All general comparator cases call the same benchmark interface. The direct
Mutex Lock/Unlock case separately measures ctxsync and sync without interface
dispatch, making it suitable for inspecting small differences in fast paths.
Locks, channels, semaphore objects and contexts are created before ResetTimer;
there is no factory or context creation in an iteration. The cancellable
context's lazy Done channel is also created during setup. It is an open
WithCancel context, cancelled only during benchmark cleanup.
These cases measure successful acquisition overhead, not cancellation latency.

The standard library has no context locking API. Its context adapter checks
ctx.Err() before calling its ordinary Lock or RLock and cannot cancel a blocked
acquisition. Treat those columns as overhead baselines. The retry comparators use
ordinary sync locks for plain callers; context callers use TryLock and retry on
buffered notifications from releases. Mutex uses the `retry` label. RW uses
`retry-serialized`: a separate mutex serializes every writer TryLock attempt,
including the public TryLock helper. That helper can wait for another short
attempt to finish. Reader TryRLock attempts need no such gate. These designs do
not provide ctxsync's fairness guarantee, and measure the alternatives' cost.
They were implemented here rather than copied from another repository.

An earlier notification-only RW comparator, labelled `retry` in the original comparator, did not complete the full benchmark run. Go 1.26.9's
`sync/rwmutex.go:169-192` allows a writer TryLock to hold the internal writer mutex
briefly, fail its reader-count CAS, and release that mutex without a public
Unlock. A deterministic synthetic negative control demonstrates how another
writer can consume the last reader's notification during that interval, fail,
and remain parked with no owner until cancellation. The writer-attempt gate
prevents that schedule; the matching synthetic progress test and native
comparator tests cover the correction. This is a benchmark-comparator defect.
The precise schedule of the recorded hang was not observed.

Mutex benchmarks count one acquire/release pair per iteration. RW parallel
benchmarks count a batch of **ten** acquire/release pairs per iteration and report
`10 locks/op`. Every batch has exactly nine reads and one write for `90r10w`, or
one read and nine writes for `10r90w`; worker phases are staggered. Thus RW
`ns/op`, `B/op` and `allocs/op` are per ten-pair batch, and should be divided by
ten when reporting a per-pair figure. Critical sections are empty in all cases.

Create the output directory with `mkdir -p ../.scratch`, then run from this
directory after the product tests and model gates pass. Do not run
while another model search, stress run or benchmark occupies the assigned CPUs.
From the repository root, `bash scripts/benchmark.sh 1s` builds one binary,
records source and binary hashes and the environment, runs both sets below,
and writes raw output and benchstat tables under `.scratch/benchmarks-*`.

```sh
taskset -c 8-23 go test ./... -count=1
taskset -c 8-23 go test -race ./... -count=1

taskset -c 8-23 go test -run '^$' \
  -bench '^BenchmarkMutex(DirectLockUnlock|Uncontended)$' \
  -benchmem -benchtime=1s -count=10 -cpu=1 > ../.scratch/bench-uncontended.txt

taskset -c 8-23 go test -run '^$' \
  -bench '^Benchmark(MutexParallel|RWParallel)$' \
  -benchmem -benchtime=1s -count=10 -cpu=4,16 > ../.scratch/bench-parallel.txt
```

The complete suite contains 90 benchmark leaves across CPU counts 1, 4 and 16,
so ten one-second samples take at least fifteen minutes plus calibration and
semaphore contention costs. Short runs can check that the harness works, but
do not establish a performance comparison.

Keep CPU counts separate in benchstat tables. `.fullname` preserves operation,
RW mix and the CPU suffix; `/impl` selects the comparator column:

```sh
~/go/bin/benchstat -col '/impl@(sync ctxsync semaphore retry retry-serialized)' \
  -row .fullname ../.scratch/bench-uncontended.txt
~/go/bin/benchstat -col '/impl@(sync ctxsync semaphore retry retry-serialized)' \
  -row .fullname ../.scratch/bench-parallel.txt
```

benchstat removes the column key from the row projection, so corresponding
implementations share a row while `-4` and `-16` remain separate. For comparisons
between revisions, keep the same toolchain, CPU affinity and arguments, then
use benchstat's default file columns on the two raw outputs. Record toolchain,
CPU model, GOMAXPROCS, affinity, sample count and benchmark duration with results.

## License

Benchmark sources are covered by [Apache-2.0](LICENSE). The imported package
retains its own license and upstream notices.
