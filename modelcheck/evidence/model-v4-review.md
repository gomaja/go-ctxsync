# Iteration 4: committed normal-wake release shortcut

| Gate | Configurations | States | Edges |
| --- | ---: | ---: | ---: |
| Mutex bounded | 18 | 11,688,189 | 41,814,761 |
| Mutex continuous traffic | 2 | 222,999 | 717,558 |
| Mutex total | 20 | 11,911,188 | 42,532,319 |
| RWMutex bounded | 16 | 2,725,247 | 10,977,348 |
| RWMutex continuous traffic | 4 | 332,730 | 1,229,770 |
| RWMutex total | 20 | 3,057,977 | 12,207,118 |

Every positive row is exhaustive and has no failure. Bounded configurations and actor/method schedules are unchanged from iteration 3: 2×2, 3×1, 3×2, and 4×1 with plain/context/mixed/Try patterns; Mutex additionally checks head requeue and cancellation during handoff. SCC configurations include every pending victim attempt, plain and cancelling Mutex traffic, and reader/writer victims with plain and cancelling RW traffic. The preserved old sendLock negative still has 457,408 states, 1,492,899 edges, and its exact 34-transition fair cycle.

The existing nondeterministic guard admission does not establish this shortcut by itself. Returning at the release Add permits the actor to start another operation while someone else is still inside the queue guard. The iteration-4 producer changes the actual `qUnlockAdd` completion transition and exhaustively checks those newly available orders.

Woken denotes a committed normal recipient, including the short interval between wake reservation and buffered signal delivery. The responsible sender finishes that bounded guarded signal sequence. The recipient then must do one of three things: acquire and provide a future release; cancel, clear its reservation and advance the next waiter; or lose, clear Woken while registering again, and either proactively advance an ownerless queue or leave advancement to the current owner. Registration preserves the recipient's old head priority and elapsed wait. The release shortcut leaves this responsibility in place. A starving release retains its own handoff obligation and joins the guard.

Five named tests exercise busy-guard release, retained starvation ownership, cancelled-recipient readiness, lost-recipient head requeue with a live owner, and aged ownerless self-handoff after two skipped releases. They remain in `../queue_test.go`.

The explicit `cancel-woken-without-advance` control clears a cancelled normal recipient's accounting and completes it without queue advancement. Its named readiness test fails with an unlocked queued victim, no Woken reservation, no owner, and no signal. From zero, the 3×1/context search finds P3 failure after 34 transitions, at 47,866 states and 143,616 edges. The initial 2×2/context search passed because a later operation can rescue the queue; the final witness uses one operation per actor. All nine prior named queue controls still fail assertions, for ten controls total. Named-control outcomes are in `queue-v6-prior-named-controls.json`; the explicit reachable witness is `queue-v4-release-cancel-woken.json`.

Production must preserve invalid-Unlock detection before returning through this shortcut. An unowned reserved handoff can have state `waiterUnit|starvingBit` (12); an illegal held-bit subtraction produces 11, which contains Woken without Starve. Checking only those two bits would incorrectly suppress the panic. The producer only enables `qUnlockAdd` for an owner; the product separately validates misuse. Its shortcut can require the released held bit to be clear along with the Woken/no-Starve test.

The packed ticket guard and fairness engine supply the composition assumptions. Their arithmetic, rank, capacity, finite-body, strong actor fairness, and weak threshold justice assumptions remain those documented in `model-v3-review.md`. Enumeration covers up to four actors and two bounded operations, with three actors in continuous traffic; the gate does not replace runtime scheduler, performance, or allocation evidence.

The complete release-shortcut results are in `queue-v4-release-gate.json` and
`rw-v4-release-gate.json`. Replay from `modelcheck/` with
`taskset -c 8-23 go run . -design queue -scenario bounded -config 3x1/context -variant cancel-woken-without-advance`
(expected exit 1). The current source includes the later fresh-call probe; its
complete gates are recorded separately.
