# sendLock loses the priority of a woken waiter

The channel queue in go-sctp's `send_linux.go` permits an exact fair starvation
cycle under continuing plain-lock traffic. The old context caller T is never
cancelled and has already waited longer than 1 ms. B and F repeatedly lock and
unlock. All three callers execute steps in every cycle; T never acquires.

The committed witness in
[`starvation.json`](../modelcheck/evidence/starvation.json) reaches
the repeated full state after transition 45 and returns to it after transition
79: **34 transitions**. This includes atomic snapshots, select commitment,
queue order, flags, tokens and live caller locals.

| Stage | State bits | Consequence |
| --- | ---: | --- |
| B owns; T waits | 9 | T is the old queued context caller |
| B releases and wakes T | 8, 10 | T's normal receive commits |
| F barges | 11 | T has not removed its registration |
| Fresh B queues | 19 | B is now ahead of T |
| T removes its old registration | 9 | T still cannot acquire |
| T registers again in starvation | 21 | The channel puts T behind B |
| F releases and hands to B | 20 | Fresh B receives ownership |
| B accepts and exits starvation | 9 | B's own wait was under 1 ms |

Source mapping, from the inspected snapshot of `send_linux.go`:

- Lines 149–159 register a waiter and preserve its first wait time.
- Lines 160–183 select the shared hand, wake and cancellation channels.
- Lines 170–179 consume a normal wake and remove the old registration.
  Losing returns to the registration/select loop, which enters the channel's
  FIFO receive queue again at its tail.
- Lines 161–168 accept starvation ownership and exit starvation when the
  recipient is the last waiter or its own wait was short.
- Lines 238–242 deliver normal wakes through the shared channel.

Go 1.26.9's `internal/sync/mutex.go`, `Mutex.lockSlow`, lines 144–150,
instead uses `queueLifo := waitStartTime != 0` and
`runtime_SemacquireMutex(..., queueLifo, ...)`: a previously woken waiter that
loses re-enters at the head. Its starvation exit test is at lines 160–169.
Replacing the FIFO channel queue with targeted waiter nodes is necessary to
preserve this priority.

The independent exhaustive SCC search also finds a 34-transition cycle with
all three actors progressing in
[`sendlock-exhaustive-liveness.json`](../modelcheck/evidence/sendlock-exhaustive-liveness.json).
Its reachable prefix differs from the directed witness above. Both use a
feasible clock schedule: age T once, then keep each fresh B wait below 1 ms
while time advances on every lap.

```sh
(cd modelcheck && taskset -c 8-23 go run . -design sendlock -scenario starvation)
(cd modelcheck && taskset -c 8-23 go run . -design sendlock -scenario liveness)
```

Both commands exit 1 with the expected counterexample. Source hashes are
recorded in [`sources.json`](../modelcheck/evidence/sources.json).
This is model evidence. No deterministic runtime reproduction of this exact
sendLock schedule was performed, and go-sctp was not modified.
