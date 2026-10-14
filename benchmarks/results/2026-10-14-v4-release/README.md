# Release shortcut comparison

Product: the release-shortcut candidate; same benchmark source as the original benchmark comparator. Go 1.26.9, CPUs 8–23, GOMAXPROCS 4/16, ten one-second samples per case. Only plain Mutex comparison was run.

The safe release shortcut regresses contention: 28.61 versus 20.68 ns/op at four workers, and 115.35 versus 27.30 at sixteen. These results remain as tuning evidence and are slower than sync.
