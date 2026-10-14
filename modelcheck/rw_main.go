// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func runRW(scenario, filter string, v variant, limit int, output string) int {
	if v != correct && v != rwForgetReaders && v != rwUndoReader && v != rwAdmitReader {
		fmt.Fprintln(os.Stderr, "unknown RW variant:", v)
		return 2
	}
	if scenario != "all" && scenario != "bounded" && scenario != "liveness" {
		fmt.Fprintln(os.Stderr, "unknown RW scenario:", scenario)
		return 2
	}
	var results []rwResult
	status := 0
	show := func(r rwResult) {
		results = append(results, r)
		fmt.Printf("RW %s %s states=%d edges=%d terminals=%d reuse=%d exhaustive=%t seconds=%.3f coverage=%v\n", v, r.Config, r.States, r.Edges, r.Terminals, r.ReuseStates, r.Exhaustive, r.Seconds, r.Coverage)
		if r.Failure != nil {
			fmt.Printf("FAIL: %s (%d transitions)\n", r.Failure.Property, len(r.Failure.Trace)-1)
			for _, step := range r.Failure.Trace {
				fmt.Printf("%03d g%d %s\n    %s\n", step.Step, step.Actor, step.Action, step.State)
			}
			status = 1
		} else if !r.Exhaustive {
			fmt.Println("INCONCLUSIVE: RW state limit")
			status = 2
		}
	}
	if scenario != "liveness" {
		for _, c := range rwConfigurations() {
			if !strings.Contains(c.Name, filter) {
				continue
			}
			show(rwExplore(c, v, limit, true))
			if status != 0 {
				break
			}
		}
	}
	if status == 0 && scenario != "bounded" {
		for _, reader := range []bool{true, false} {
			for _, cancelling := range []bool{false, true} {
				show(rwLiveness(reader, cancelling, v, limit))
				if status != 0 {
					break
				}
			}
			if status != 0 {
				break
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
