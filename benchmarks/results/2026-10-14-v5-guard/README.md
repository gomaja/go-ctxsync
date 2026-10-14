# Bounded guard polling comparison

Iteration 5 adds at most 64 fresh head polls before Gosched to the iteration-4 release shortcut. Same benchmark source as the original benchmark comparator; Go 1.26.9, CPUs 8–23, GOMAXPROCS 4/16, ten one-second samples per case. Only plain Mutex comparison was run. The measurement ran without other concurrent CPU-intensive validation.

Median ctxsync versus sync: 24.41 versus 18.09 ns/op at 4 workers (+34.91%), and 139.60 versus 25.96 ns/op at 16 (+437.75%). This candidate is slower than sync under contention. The binary digest and raw output are preserved. These measurements precede the fresh-call probe. Binary hashes identify the measured executable; the retained correctness reviews describe the corresponding guard and queue protocols.
