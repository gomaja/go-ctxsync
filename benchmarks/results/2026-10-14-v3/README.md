# Initial measured design

Go 1.26.9, Linux/amd64, CPUs 8–23, ten one-second samples. The immutable binary
used the initial package candidate and the original benchmark comparator;
source hashes are in `environment.txt`.
The product did not yet skip releases with an already assigned normal wake.

The uncontended set completed. The parallel set **did not complete**: it exited
2 with all goroutines asleep in the notification-only RW retry comparator at
`90r10w/ContextCancellable/impl=retry-4`. The stack dump is in `failure.log`.
Completed rows retain ten samples; missing rows have no numerical result. The
incomplete table excludes the unfinished line and must not be treated as a full
suite. The later `retry-serialized` RW comparator is a separate, named remedy.

Direct Mutex Lock/Unlock is 8.250 ns/op versus sync's 8.216 (p=0.928, n=10).
Plain contention is about 23.78 versus 19.36 ns/op at four workers, and 38.85
versus 26.675 at sixteen, so this design is slower than sync under contention.
A separate two-second CPU profile shows scheduler/yield work dominates
sampled CPU time; its timings are diagnostic and are not comparison samples.
