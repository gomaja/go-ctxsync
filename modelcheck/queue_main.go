// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func runQueue(scenario, filter string, v variant, limit int, output string) int {
	switch v {
	case correct, suppressWakeHeld, requeueTail, exitIgnoresAge, dropHandedOwnership, leaveCancelledNode, cancelWokenNoAdvance, reReadMode, lostWake, lazyPublish, tryIgnoresStarve, probeIgnoresStarve, probeDropsAccounting, probeRetries:
	default:
		fmt.Fprintln(os.Stderr, "unknown queue variant:", v)
		return 2
	}
	if scenario != "bounded" && scenario != "liveness" && scenario != "all" {
		fmt.Fprintln(os.Stderr, "unknown queue scenario:", scenario)
		return 2
	}
	var results []queueResult
	status := 0
	for _, c := range queueConfigurations() {
		if scenario == "liveness" {
			break
		}
		if !strings.Contains(c.Name, filter) {
			continue
		}
		r := queueExplore(c, v, queueInitial(c), limit, true)
		results = append(results, r)
		fmt.Printf("queue %s %s states=%d edges=%d terminals=%d exhaustive=%t seconds=%.3f coverage=%v\n", v, c.Name, r.States, r.Edges, r.Terminals, r.Exhaustive, r.Seconds, r.Coverage)
		if r.Failure != nil {
			displayQueueFailure(r)
			status = 1
			break
		}
		if !r.Exhaustive {
			fmt.Println("INCONCLUSIVE: state limit")
			status = 2
			break
		}
	}
	if status == 0 && scenario != "bounded" {
		for _, cancelTraffic := range []bool{false, true} {
			r := queueLiveness(v, limit, cancelTraffic)
			results = append(results, r)
			fmt.Printf("%s %s states=%d edges=%d exhaustive=%t seconds=%.3f\n", r.Config, v, r.States, r.Edges, r.Exhaustive, r.Seconds)
			if r.Failure != nil {
				displayQueueFailure(r)
				status = 1
				break
			}
			if !r.Exhaustive {
				status = 2
				break
			}
		}
		if status == 0 {
			r := sendLockLiveness(limit)
			display(r)
			if !r.Exhaustive {
				status = 2
			} else if r.Failure == nil || r.CycleLength == 0 {
				fmt.Println("FAIL: old sendLock negative control has no fair cycle")
				status = 1
			}
			if output != "" {
				data, err := json.MarshalIndent(r, "", "  ")
				if err == nil {
					err = os.WriteFile(output+".sendlock.json", append(data, '\n'), 0o600)
				}
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					status = 2
				}
			}
		}
	}
	if len(results) == 0 {
		status = 2
	}
	if output != "" {
		data, err := json.MarshalIndent(results, "", "  ")
		if err == nil {
			err = os.WriteFile(output, append(data, '\n'), 0o600)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			status = 2
		}
	}
	return status
}

func queueConfigurations() []config {
	out := configurations()
	for _, pattern := range []struct {
		name  string
		kinds [3]kind
	}{
		{"3x2/head-requeue", [3]kind{contextLock, plainLock, plainLock}},
		{"3x2/head-cancellation-handoff", [3]kind{plainLock, contextLock, contextLock}},
	} {
		c := config{Name: pattern.name, Actors: 3, Ops: 2}
		for i, k := range pattern.kinds {
			for op := 0; op < 2; op++ {
				c.Kinds[i][op] = k
			}
		}
		out = append(out, c)
	}
	return out
}

func displayQueueFailure(r queueResult) {
	fmt.Printf("FAIL: %s (%d transitions)\n", r.Failure.Property, len(r.Failure.Trace)-1)
	for _, step := range r.Failure.Trace {
		fmt.Printf("%03d g%d %s\n    %s\n", step.Step, step.Actor, step.Action, step.State)
	}
}
