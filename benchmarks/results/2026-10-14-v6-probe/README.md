# Single fresh-probe comparison

Iteration 6 retains the normal-Woken release shortcut and bounded guard polling, and adds one context-checked full-state Load/CAS probe before registration. Same benchmark source as the original benchmark comparator; Go 1.26.9, CPUs 8–23, GOMAXPROCS 4/16, ten one-second samples per case. Only plain Mutex comparison was run. The measurement ran without other concurrent CPU-intensive validation.

Median ctxsync versus sync: 15.18 versus 18.07 ns/op at 4 workers (-16.00%), and 15.25 versus 26.66 ns/op at 16 (-42.81%). This comparison is faster than sync under plain contention. Environment, exact source/binary digests and raw measurements are preserved. The complete suite uses the corrected RW comparator and is preserved separately.
