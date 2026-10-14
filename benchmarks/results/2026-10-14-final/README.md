# Complete benchmark results

`bash scripts/benchmark.sh 1s` completed successfully on 2026-10-14. One immutable binary measured all 90 leaves with ten one-second samples each: 18 uncontended and 72 parallel. CPUs 8–23; GOMAXPROCS 1 for uncontended and 4/16 for parallel. The measurement ran without other concurrent CPU-intensive validation.

The measured product uses the single fresh-call probe; the RW comparator serializes writer attempts. Source digests identify the files measured before the checker relocation and documentation cleanup. Library comments changed afterward; executable lock behavior and the final comparator source are unchanged. Raw output, environment and source hashes, binary digest, benchstat and medians are retained. The compiled binary remains checkout-local in the original scratch result directory and is not committed.

RW measurements count ten acquire/release pairs per operation, with exactly nine reads/one write or one read/nine writes. Divide ns/op by ten for per-pair figures. The standard-library context adapters check entry error but cannot cancel a blocked lock. The RW retry comparator is labelled retry-serialized and serializes writer attempts; its public Try helper may wait for a short attempt. Mutex retry is unchanged.

All 90 leaves have ten samples, and every ctxsync sample reports zero B/op and allocs/op. See the main README for selected ns/op medians and both benchstat files for all cases, confidence intervals, comparisons and allocation results. These results apply to this host and workload; earlier regressions and the original retry-comparator failure remain in separate result directories.
