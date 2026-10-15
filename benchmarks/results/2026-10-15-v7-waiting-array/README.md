# Waiting-array guard comparison

The comparison base is `de26fb4`, which lets every claim poll the packed ticket word. Latency also retains the rejected distance-proportional policy as `fixed`. In the throughput files, `fix` names the waiting-array guard, not that rejected policy.

The main throughput, latency, race and RW-rerun measurements used the waiting-array candidate with 32-bit slot counters (`guard.go` at `a2c523e`, SHA-256 `13295fe57488bf720916deccbf7309ffc235b73c59b7f5a5edf69258f591bcd0`, retained as `latency/final-guard.go.txt`). Review then found that a 32-bit slot can return to a waiting claim's saved value after 2^32 colliding increments, so the final guard uses 64-bit slots, as in the TWA paper. That is its only difference; [final-64bit-slots/](final-64bit-slots) re-measures the final source (SHA-256 `a773cbb9eec64d9e6e532a51fac29852722a958c3d94c323eaee36494ef47fed`, retained as `final-64bit-slots/final-guard.go.txt`).

## Throughput

The [driver](throughput/run.sh) alternated base and final binaries for ten one-second samples per case, on CPUs 16–31 with Go 1.26.9, Linux/amd64 and an AMD Ryzen AI Max+ 395. Uncontended cases use GOMAXPROCS=1; parallel cases use 4 and 16. The parallel comparison includes sync and ctxsync only; uncontended Mutex cases also include semaphore and retry controls. There are 54 cases and 540 samples per binary, including 230 ctxsync samples. All ctxsync samples report zero B/op and zero allocs/op.

Critical sections are empty. A Mutex operation is one acquire/release pair; an RW operation is ten pairs with exactly nine reads and one write, or one read and nine writes. The cancellable context is already initialized and remains open during timing. Standard-library context adapters only check the context on entry; they cannot cancel a blocked acquisition. These cases measure successful-acquisition throughput, not cancellation latency (`../../lock_test.go:230-362`).

The machine was shared with other work. [environment.txt](throughput/environment.txt) records load at the start and end; [progress.log](throughput/progress.log) records all ten sample boundaries. CPU affinity does not establish isolation: latency measurements used CPUs 0–15, and other work could use either set. The start/end 1/5/15-minute load averages were 35.02/17.14/18.74 and 26.56/28.91/24.67; one-minute readings at sample boundaries ranged from 5.88 to 60.92. The load readings and the full sample distributions should accompany any comparison.

[Raw base](throughput/base.txt), [raw final](throughput/fix.txt) and [benchstat](throughput/benchstat.txt) retain every sample and metric. The table below lists every significant ns/op difference reported by benchstat (unadjusted p<=0.05, n=10 per binary); positive deltas mean more time. The `n` suffix denotes nanoseconds. All allocation metrics are unchanged at zero; RW `locks/op` is unchanged at ten. These are per-case tests without a multiple-comparison correction.

| Case | Base | Final | Delta | p |
| --- | ---: | ---: | ---: | ---: |
| `RWParallel/mix=10r90w/operation=ContextBackground/impl=ctxsync-4` | 357.3n | 371.6n | +4.02% | 0.009 |
| `RWParallel/mix=10r90w/operation=ContextBackground/impl=ctxsync-16` | 362.0n | 377.2n | +4.19% | 0.029 |

Ctxsync throughput has significant regressions in the cases listed above; the root README benchmark section has not been extended with a throughput claim.

## Cancellation and guard-drain latency

The [harness](latency/review_perf_test.go.txt) and [driver](latency/runfinal.py) ran fresh processes on CPUs 0–15. The Go harness is retained byte-for-byte with a `.txt` suffix so it is not compiled as a benchmark package. `P` is GOMAXPROCS, `N` is queued claims, and load is the number of CPU-bound goroutines in the measured process. P=64 deliberately oversubscribes the 16 allowed CPUs. Each ordinary cell has three samples per variant; saturated cancellation cells request eight. The driver rotates the variant order for the first three samples, then uses base/fixed/final order. After a nonzero exit, that variant is skipped for the remaining samples of the same cell. A timeout therefore has no replacement samples.

Cancellation timing starts immediately before cancelling one shared context and ends after all N callers return `context.Canceled`; registration and background-load setup precede timing. The guard-drain test times a release followed by completion of N queued guard claims. Race runs use `GORACE=atexit_sleep_ms=0`. Each process has a 15-second watchdog covering setup and timed work; TIMEOUT means exit 2 without an elapsed value, not a measured 15-second cancellation latency. The driver discards individual process stdout/stderr, retaining exit codes and parsed elapsed values in [runs.json](latency/runs.json); [summary.txt](latency/summary.txt) retains its original grouped output. [environment.txt](latency/environment.txt) records the harness and six executable digests, CPU and initial load (31.06/17.86/18.95 for 1/5/15 minutes).

Every recorded sample is shown below in milliseconds, in driver sample order. Missing samples after a timeout were skipped, not successful. There are 195 process records: 190 successful elapsed values and five exits with code 2 and no elapsed value, all in base race cells (labelled TIMEOUT). All final processes succeeded.

| Test | Mode | P | N | Load | Base samples (ms) | Rejected fixed samples (ms) | Final samples (ms) |
| --- | --- | ---: | ---: | ---: | --- | --- | --- |
| CancellationRegime | native | 1 | 128 | 0 | 0.071, 0.054, 0.052 | 0.087, 0.053, 0.053 | 0.072, 0.054, 0.062 |
| CancellationRegime | native | 1 | 512 | 0 | 0.244, 0.247, 0.361 | 0.228, 0.185, 0.323 | 0.191, 0.376, 0.270 |
| CancellationRegime | native | 16 | 512 | 0 | 22.626, 28.147, 29.773 | 37.010, 56.160, 53.993 | 26.605, 34.436, 28.544 |
| CancellationRegime | native | 64 | 512 | 0 | 44.595, 36.803, 30.564 | 65.631, 64.328, 58.027 | 33.664, 41.961, 41.685 |
| CancellationRegime | native | 16 | 128 | 16 | 60.662, 40.559, 40.510 | 40.509, 40.475, 40.413 | 40.543, 40.449, 40.473 |
| CancellationRegime | native | 64 | 512 | 64 | 2816.433, 4302.889, 3499.946, 3592.006, 3602.215, 3476.005, 4646.627, 3328.077 | 7255.597, 5791.070, 4438.112, 6320.070, 6746.138, 6499.906, 6998.189, 4784.150 | 4516.478, 64.004, 4127.471, 88.720, 3581.117, 4697.862, 4359.732, 3791.707 |
| CancellationRegime | race | 1 | 128 | 0 | 0.240, 0.235, 0.258 | 0.288, 0.258, 0.230 | 0.236, 0.243, 0.447 |
| CancellationRegime | race | 1 | 512 | 0 | 1.735, 1.431, 1.772 | 1.370, 0.997, 1.739 | 1.171, 1.180, 1.150 |
| CancellationRegime | race | 16 | 512 | 0 | TIMEOUT | 66.132, 69.383, 61.626 | 40.398, 35.700, 41.803 |
| CancellationRegime | race | 64 | 512 | 0 | TIMEOUT | 74.911, 68.135, 68.967 | 39.842, 45.383, 41.001 |
| CancellationRegime | race | 16 | 128 | 16 | 40.543, 60.653, 46.406 | 60.938, 40.561, 50.403 | 40.610, 46.171, 44.074 |
| CancellationRegime | race | 64 | 512 | 64 | TIMEOUT | 2892.054, 4155.978, 1384.332, 5151.968, 3859.950, 5408.543, 794.258, 5276.023 | 2824.058, 3142.264, 2897.003, 2352.222, 985.724, 3.268, 163.946, 2268.012 |
| GuardDrain | native | 1 | 128 | 0 | 0.355, 0.311, 0.329 | 0.843, 0.873, 0.776 | 0.350, 0.321, 0.342 |
| GuardDrain | native | 1 | 512 | 0 | 1.444, 1.708, 1.687 | 11.524, 11.811, 12.994 | 1.644, 1.607, 1.783 |
| GuardDrain | native | 16 | 512 | 0 | 27.136, 40.499, 35.094 | 41.773, 58.676, 80.926 | 28.500, 42.438, 45.964 |
| GuardDrain | native | 64 | 512 | 0 | 63.717, 80.691, 85.440 | 83.616, 84.359, 79.292 | 76.595, 46.575, 67.786 |
| GuardDrain | race | 1 | 128 | 0 | 12.766, 13.016, 12.830 | 1.172, 1.125, 1.057 | 5.881, 4.498, 7.545 |
| GuardDrain | race | 1 | 512 | 0 | 190.989, 157.365, 182.498 | 20.865, 17.311, 17.998 | 77.026, 69.442, 66.230 |
| GuardDrain | race | 16 | 512 | 0 | TIMEOUT | 71.786, 79.202, 64.252 | 39.663, 44.950, 47.896 |
| GuardDrain | race | 64 | 512 | 0 | TIMEOUT | 111.001, 119.261, 117.051 | 78.842, 48.990, 187.951 |

The saturated final native cancellation samples are bimodal: two complete in 64.004 and 88.720 ms, while six take 3581.117–4697.862 ms. The native saturated medians are 3545.976 ms for base, 6409.988 ms for rejected fixed and 3959.589 ms for final; final does not consistently improve this regime. Saturated race final ranges from 3.268 to 3142.264 ms (median 2310.117 ms), while base's first attempt is labelled TIMEOUT. This spread is scheduler- and load-sensitive; a fast sample or a median alone does not bound tail latency.

With one P and 512 claims, native guard-drain samples are 1.444–1.708 ms for base, 11.524–12.994 ms for rejected fixed, and 1.607–1.783 ms for final. Final avoids the rejected policy's extra drain delay in these samples. At 16 and 64 Ps with 512 callers and no background load, base race cancellation is labelled TIMEOUT, while final takes 35.700–45.383 ms. Small sample counts and shared load limit claims outside these recorded cells.

## Provenance and limits

Files under `throughput/` and `latency/` were copied from scratch measurement directories after each run finished; `race/` and `throughput-rw-rerun/` likewise. Local directory prefixes in drivers and environment records were replaced with placeholders such as `<fix checkout>`; recorded digests are unchanged. The retained guard snapshots distinguish every measured variant. Executable SHA-256 values are preserved in the environment records; no binaries are included. `SHA256SUMS` covers every retained file except itself. The drivers, environment records and raw outputs are the command and source provenance for these measurements.

The RW rerun below is the only repeated throughput measurement. Throughput has no race instrumentation, cancellation, nonempty critical sections or parallel semaphore/retry comparison. The latency harness samples only Mutex cancellation and the internal guard, with one shared context and fixed queue sizes; it does not measure RW reader cancellation, arbitrary holder delays, allocation profiles, other architectures, or an idle machine. The 15-second process watchdog is not a per-caller latency bound. These measurements do not prove unconditional scheduler fairness or a performance bound.

## Race-detector stall (issue #1)

[race/run.sh](race/run.sh) ran fresh race-instrumented processes with GOMAXPROCS=32 on CPUs 17–31 (15 CPUs), the issue's configuration, while the machine carried other work (load 26.56 to 46.02 at the start and end, [environment.txt](race/environment.txt)). The [reproducer](race/repro_test.go.txt) is the issue's program: 128 `LockContext` callers queue on a held Mutex, one context is cancelled, and each result must arrive within three seconds of the previous one.

| Variant | Runs passing the watchdog | Wall time per run (s), including the race runtime's exit delay |
| --- | ---: | --- |
| base | 10 of 10 | 19.07, 25.47, 19.99, 37.90, 26.22, 31.01, 17.87, 22.19, 18.46, 22.50 |
| final | 10 of 10 | 1.16, 1.17, 1.15, 1.14, 1.14, 1.14, 1.13, 1.15, 1.15, 1.16 |

The race runtime waits one second at exit by default and the reproducer sleeps 100 ms before cancelling, so a final run of about 1.15 s spends milliseconds on cancellation. The base runs passed this watchdog but each took 18 to 38 seconds; in the issue's own runs 3 of 10 failed it.

[instr_test.go.txt](race/instr_test.go.txt) runs the same workload against copies of the base and final guards that record timestamps around ticket allocation and release ([base copy](race/instrumented-base-guard.go.txt), [final copy](race/instrumented-final-guard.go.txt)). Three runs each, same configuration:

| Variant | Release CAS p99 / max | Ticket Add max | All cancellations returned after cancel |
| --- | --- | --- | --- |
| base | 2072 / 3115 ms, 1232 / 2875 ms, 2234 / 2690 ms | 1140, 760, 2296 ms | 30.9, 26.1, 27.7 s |
| final | 0.027 / 0.196 ms, 0.030 / 0.062 ms, 0.027 / 0.511 ms | 0.010, 0.014, 0.024 ms | 19.4, 12.3, 5.1 ms |

Guarded bodies were short in both; the base time went into the release CAS and ticket Add that every polling waiter delayed. The instrumentation itself adds clock reads to each guard section.

## Contended RW throughput, quieter rerun

[throughput-rw-rerun/run.sh](throughput-rw-rerun/run.sh) alternated base, final and a variant with a stride-17 slot hash ([stride-guard.go.txt](throughput-rw-rerun/stride-guard.go.txt)) for eight 500 ms samples of every contended ctxsync RW case. The driver records neither its arguments nor the load; [environment.txt](throughput-rw-rerun/environment.txt) reconstructs them (CPUs 16–31, one-minute load 8.98 at the start, no end reading) with the binary digests. Final against base: geomean +2.49 %, significant for `mix=90r10w/operation=Plain` at 4 workers (232.8 to 244.1 ns, +4.85 %, p=0.041) and 16 workers (238.5 to 250.7 ns, +5.14 %, p=0.026); no other case differed significantly ([benchstat](throughput-rw-rerun/cmp-s17-final.txt)). Stride 17 against final: geomean +0.21 %, no case significant ([benchstat](throughput-rw-rerun/cmp-s17-final-vs-stride.txt)). Stride 17 showed no improvement in this run, so the final guard keeps consecutive slots. Across these two throughput runs, contended RW cases cost up to about five percent and Mutex cases did not change significantly; the quieter final-guard run below measured up to about nine percent.

## Final guard: 64-bit slots

The [throughput driver](final-64bit-slots/throughput/run.sh) alternated base, the 32-bit candidate and the final guard for eight 500 ms samples of every contended ctxsync RW and Mutex case on CPUs 16–31, at the lowest load of these runs (one-minute 3.57 at the start and 6.42 at the end, [environment.txt](final-64bit-slots/throughput/environment.txt)). The 32-bit candidate against the final guard: geomean +0.17 %, no case significant ([benchstat](final-64bit-slots/throughput/cmp-w64-final32-final64.txt)), so the slot width costs nothing measurable. Base against the final guard ([benchstat](final-64bit-slots/throughput/cmp-w64-base-final64.txt)): no Mutex case changed significantly, and 8 of 12 contended RW cases took significantly longer, by +1.88 % to +8.98 %. Read-mostly cases cost the most (`mix=90r10w`: +5.36 % to +8.98 % where significant) and write-mostly cases +1.88 % to +4.62 %. This quieter run therefore puts the contended RW cost at up to about 9 %.

The race evidence was repeated with the final guard on CPUs 0–14 with GOMAXPROCS=32 ([race/](final-64bit-slots/race)): the issue's reproducer passed 10 of 10 runs in 1.13–1.17 s, and the instrumented copy measured release CAS at most 0.134, 0.110 and 0.609 ms, ticket Add at most 0.031 ms, and all cancellations returned 19.6, 1.6 and 6.2 ms after cancel. The [latency regimes](final-64bit-slots/latency/summary.txt) for the final guard alone match the 32-bit candidate's ranges above: for example, race cancellation of 512 callers took 31.7–50.8 ms at 16 and 64 Ps, and a one-P native drain of 512 claims 1.44–2.06 ms. The saturated native regime stayed bimodal (42.7 ms to 4.15 s). These latency and race runs were made at a different time from the main runs, so they support similarity, not a paired comparison.
