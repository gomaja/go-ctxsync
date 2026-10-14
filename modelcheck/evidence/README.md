# Retained correctness evidence

These records separate the current finite gates from historical counterexamples.
Every graph records its configuration, transition variant, scope and state/edge
counts. The current source is in the parent directory; commands below run from
`modelcheck/`. Earlier candidates can have different graph counts and trace
phases. A negative result is a required control, not a passing protocol.

| Claim | Records |
| --- | --- |
| Current Mutex safety, completion, reuse and fair-cycle gates | [queue-v6-probe-gate.json](queue-v6-probe-gate.json) |
| Current composed RWMutex gates | [rw-v6-probe-gate.json](rw-v6-probe-gate.json) |
| Independent packed ticket guard, 44 configurations and wrap checks | [guard-v5-totals.json](guard-v5-totals.json), [separate-counter witness](guard-v5-separate-try-wrap.json) |
| Old sendLock starvation, exact 34-transition cycle | [directed witness](starvation.json), [exhaustive search](sendlock-exhaustive-liveness.json) |
| Why wake suppression while a barger holds is unsafe | [queue-iteration-1.json](queue-iteration-1.json), [named suppression control](queue-v3-suppress-wake-while-held.json) |
| Fairness criterion, ticket capacity and composition argument | [model-v3-review.md](model-v3-review.md) |
| Normal-wake release shortcut and cancellation advancement | [model-v4-review.md](model-v4-review.md), [Mutex gate](queue-v4-release-gate.json), [RWMutex gate](rw-v4-release-gate.json), [broken advancement](queue-v4-release-cancel-woken.json) |
| Fresh head polling and cooperative yield | [model-v5-guard-review.md](model-v5-guard-review.md), [polling mutations](guard-v5-poll-mutations.json) |
| Single fresh-call probe, full-state preservation and bounded retry | [model-v6-probe-review.md](model-v6-probe-review.md), [ignoring starvation](queue-v6-probe-starvation.json), [dropping accounting](queue-v6-probe-accounting.json), [retry cycle](queue-v6-probe-retry-liveness.json), [new named controls](queue-v6-probe-named-controls.json), [prior named controls](queue-v6-prior-named-controls.json) |
| Queue accounting and age assertion controls | [queue-v3-mutations.json](queue-v3-mutations.json) |
| Named fairness-engine and guard assertion controls | [all-engine-mutations.json](all-engine-mutations.json) |
| Package source and continuous-traffic producer mutations killed by regressions | [runtime-mutations.json](runtime-mutations.json) |
| Upstream source snapshot identifiers | [sources.json](sources.json) |

The older queue controls retain these reachable failure traces:

- [Tail requeue](queue-v3-requeue-tail.json)
- [Dropped cancelled ownership](queue-v3-cancel-drops-handoff.json)
- [Cancelled node left queued](queue-v3-cancel-leaves-node.json)
- [Missing wake signal](queue-v3-omit-wake-send.json)
- [Overwritten lazy queue](queue-v3-overwrite-lazy-publication.json)
- [Release mode reread](queue-v3-reread-release-mode.json)
- [TryLock ignoring starvation](queue-v3-try-ignores-starvation.json)

The older RW controls retain [writer cancellation forgetting readers](rw-v3-writer-cancel-forgets-readers.json),
[reader-count undo error](rw-v3-reader-count-undo-error.json) and
[reader admission past a pending writer](rw-v3-reader-past-pending-writer.json).
Historical sendLock controls retain [stale Woken](sendlock-v3-leave-stale-woken.json),
[missing wake](omit-wake-send.json), [overwritten publication](overwrite-lazy-publication.json),
[release reread](reread-release-mode.json) and [TryLock ignoring starvation](try-ignores-starvation.json).
The stale-Woken case is a seeded historical cleanup invariant.

```sh
taskset -c 8-23 go run . -design queue -limit 16000000
taskset -c 8-23 go run . -design rw -limit 16000000
taskset -c 8-23 go test . -run '^TestTicketGuard' -count=1 -v
```

The old sendLock control must exit 1 with its starvation cycle:

```sh
taskset -c 8-23 go run . -design sendlock -scenario starvation
taskset -c 8-23 go run . -design sendlock -scenario liveness -limit 16000000
```

Model mutation records contain the changed source text or variant name and the
focused test command. Run package mutations separately from the repository root
with `taskset -c 8-23 python3 scripts/mutate.py`. Historical timing observations
are not performance claims. Runtime tests and benchmark measurements remain
separate from the finite graph checks.
