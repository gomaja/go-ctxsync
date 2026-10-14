# Iteration 6: one fresh-call probe before queue initialization

The producer now permits one full-state Load/CAS probe after the initial fast Lock CAS fails and before lazy queue initialization. A separate context error check precedes the probe. A busy observation or a lost CAS enters the existing queue protocol immediately; it never repeats the probe. The candidate retains the iteration-4 normal-Woken release shortcut and iteration-5 guard polling model. Previously registered or woken callers continue through their existing guarded paths and do not perform this fresh-call probe.

| Gate | Configurations | States | Edges |
| --- | ---: | ---: | ---: |
| Mutex bounded | 18 | 19,358,146 | 71,768,490 |
| Mutex continuous traffic | 2 | 471,871 | 1,597,677 |
| Mutex total | 20 | 19,830,017 | 73,366,167 |
| RWMutex bounded | 16 | 3,008,319 | 12,110,538 |
| RWMutex continuous traffic | 4 | 509,172 | 1,912,585 |
| RWMutex total | 20 | 3,517,491 | 14,023,123 |

Every positive row is exhaustive and reports no failure. The largest Mutex configuration contains 6,372,073 states, below the 16,000,000 per-configuration limit. Configurations, methods, repeat schedules, cancellation environments, and reuse checks are unchanged. The old sendLock negative still reproduces its exact 34-transition fair cycle.

Three phases make the candidate explicit: `qProbeCheck` observes context cancellation, `qProbeLoad` loads the complete atomic state, and `qProbeCAS` compares that complete snapshot and sets Held while preserving every other bit. Held or Starve in the loaded snapshot causes immediate fallthrough to `qInitLoad`. A CAS loss also falls through there. The stored Old/New values survive normalization only until that CAS; probe success leaves ownership at `qCritical` without allocating, publishing, or taking a queue guard. The initial fast CAS remains unchanged, and TryLock has no new probe.

Preserving Woken leaves the committed normal recipient responsible for its existing advancement obligation. Preserving waiter count leaves every queued or reserved waiter accounted for. The guard-stable accounting fields may change between the probe Load and CAS, but any such change makes a full-state comparison fail. If a probe succeeds before an aged guarded registrar sets Starve, that registrar's fixed Add preserves the newly acquired Held bit and the new owner's release follows the pinned starvation mode. If registration observed no owner before the probe acquired, its existing proactive normal wake remains valid across this barging owner. The probe therefore changes normal competition without consuming a wake reservation or bypassing starving ownership.

The context check is a separate transition so cancellation can interleave with the subsequent Load and CAS. A context already cancelled at that check completes without ownership or queue initialization. Cancellation after the check may race a successful acquisition, as the public contract permits. The probe never starts or resets waiting time. The existing all-pending victim predicate includes every check/Load/CAS state; physical threshold events remain disabled until first enqueue.

The one-attempt bound is essential to conditional liveness. An actor executes finitely many probe steps before it either owns the lock or joins the existing fair guard and queue protocol. A retry-on-CAS-loss variant admits a fair pre-registration cycle: competing actors repeatedly acquire between the victim's Load and CAS, and the victim repeatedly fails while still making progress. The checker finds this exact 22-transition cycle after a 12-transition prefix in a complete graph of 70,556 states and 202,954 edges. The uncancelled victim remains unregistered throughout the cycle, so no nonexistent age event is used to dismiss it.

Seven seeded tests in `../queue_test.go` cover normal Woken/count preservation while bypassing a busy guard, one-attempt CAS-loss fallthrough, rejection of starving ownership, rejection of a held state, cancellation before the check, cancellation racing after the check, and loss after accounting changes while Held remains clear.

All three new controls fail named assertions and produce graph evidence. Ignoring Starve reaches `fairness: fresh probe bypassed starvation` after 36 transitions (157,545 states / 409,069 edges, 3×2/plain). Replacing the whole preserved state with Held alone reaches P1 negative state after 22 transitions (3,015 / 6,973, 2×2/context). Retrying a lost CAS produces the pre-registration fair cycle described above. All ten prior named queue controls still fail assertions. Exact commands and results are in `queue-v6-probe-named-controls.json` and `queue-v6-prior-named-controls.json`; graph witnesses are `queue-v6-probe-{starvation,accounting,retry-liveness}.json`.

Replay with `taskset -c 8-23 go build -o model-v6 .`, then `taskset -c 8-23 ./model-v6 -design queue -scenario all -limit 16000000 -output queue.json` and the same executable command with `-design rw`. Retry negative replay uses `-design queue -scenario liveness -variant probe-retries-cas-loss`; expected exit status is 1. Accounting negative replay uses `-scenario bounded -config 2x2/context -variant probe-drops-accounting`; starvation replay uses `-scenario bounded -config 3x2/plain -variant probe-ignores-starvation`. The new seeded tests are selected with `go test . -run '^TestQueueFreshProbe' -count=1 -v`, confined to CPUs 8–23.

Finite enumeration covers up to four actors and two bounded operations, with three actors in continuous traffic. The ticket capacity, finite guarded-body, strong actor fairness, and weak threshold justice assumptions remain those documented in earlier reviews. Native scheduler, cooperative-yield-budget, allocation, and performance checks are separate evidence. The retained graph witnesses record their design and variant; use the current checker sources for replay.

Complete positive results are in `queue-v6-probe-gate.json` and
`rw-v6-probe-gate.json`. The old sendLock negative result is retained once as
`sendlock-exhaustive-liveness.json`.
