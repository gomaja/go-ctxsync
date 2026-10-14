// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Command modelcheck explores the proposed lock before it is implemented.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	design := flag.String("design", "queue", "queue, rw, or sendlock (preserved negative control)")
	filter := flag.String("config", "", "substring of configuration name")
	mutant := flag.String("variant", string(correct), "transition variant")
	limit := flag.Int("limit", 16000000, "maximum states per configuration; reaching this is inconclusive")
	output := flag.String("output", "", "write detailed JSON results here")
	scenario := flag.String("scenario", "all", "all, bounded, liveness, starvation, or exit-normalization")
	flag.Parse()
	v := variant(*mutant)
	if *design == "queue" {
		os.Exit(runQueue(*scenario, *filter, v, *limit, *output))
	}
	if *design == "rw" {
		os.Exit(runRW(*scenario, *filter, v, *limit, *output))
	}
	if *design != "sendlock" {
		fmt.Fprintln(os.Stderr, "unknown design:", *design)
		os.Exit(2)
	}
	switch v {
	case correct, reReadMode, staleWoken, lostWake, lazyPublish, tryIgnoresStarve:
	default:
		fmt.Fprintln(os.Stderr, "unknown variant:", v)
		os.Exit(2)
	}
	if *scenario == "liveness" && v != correct {
		fmt.Fprintln(os.Stderr, "unsupported sendlock liveness variant:", v)
		os.Exit(2)
	}
	if *scenario == "liveness" {
		r := sendLockLiveness(*limit)
		display(r)
		status := 0
		if r.Failure != nil {
			status = 1
		}
		if !r.Exhaustive {
			status = 2
		}
		if *output != "" {
			data, err := json.MarshalIndent(r, "", "  ")
			if err == nil {
				err = os.WriteFile(*output, append(data, '\n'), 0o600)
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				status = 2
			}
		}
		os.Exit(status)
	}
	var results []result
	status := 0
	if *scenario != "all" && *scenario != "bounded" && *scenario != "starvation" && *scenario != "exit-normalization" {
		fmt.Fprintln(os.Stderr, "unknown scenario:", *scenario)
		os.Exit(2)
	}
	if *scenario == "exit-normalization" {
		r := normalizationWitness(v)
		results = append(results, r)
		display(r)
		if r.Failure != nil {
			status = 1
		}
	} else if *scenario != "bounded" && v == correct {
		r := starvationWitness()
		results = append(results, r)
		display(r)
		status = 1
	}
	for _, c := range configurations() {
		if status != 0 || *scenario == "starvation" || *scenario == "exit-normalization" {
			break
		}
		if !strings.Contains(c.Name, *filter) {
			continue
		}
		r := explore(c, v, initial(c), *limit)
		if r.Exhaustive && r.Failure == nil {
			checkReuse(&r, v, *limit)
		}
		results = append(results, r)
		display(r)
		if r.Failure != nil {
			status = 1
			break
		}
		if !r.Exhaustive {
			fmt.Println("INCONCLUSIVE: state limit reached")
			status = 2
			break
		}
	}
	if len(results) == 0 {
		fmt.Fprintln(os.Stderr, "no matching configurations")
		status = 2
	}
	if *output != "" {
		data, err := json.MarshalIndent(results, "", "  ")
		if err == nil {
			err = os.WriteFile(*output, append(data, '\n'), 0o600)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			status = 2
		}
	}
	os.Exit(status)
}

func display(r result) {
	fmt.Printf("%s %s states=%d edges=%d terminals=%d exhaustive=%t seconds=%.3f\n", r.Variant, r.Config, r.States, r.Edges, r.Terminals, r.Exhaustive, r.Seconds)
	if r.Failure != nil {
		fmt.Printf("FAIL: %s (%d transitions)\n", r.Failure.Property, len(r.Failure.Trace)-1)
		if r.CycleLength > 0 {
			fmt.Printf("exact cycle starts after transition %d, length=%d, actors=%v, victim acquisitions=%d\n", r.CycleStart, r.CycleLength, r.CycleActors, r.VictimAcquisitions)
		}
		for _, step := range r.Failure.Trace {
			fmt.Printf("%03d g%d %s\n    %s\n", step.Step, step.Actor, step.Action, step.State)
		}
	}
}
