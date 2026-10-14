// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestFairCycleFindsHiddenFairSubcomponent(t *testing.T) {
	// Nodes 0 and 1 form one SCC. Visiting 0 enables actor 1, whose
	// progress leaves the pending-victim subgraph. The fair self-loop at 1
	// must survive pruning node 0 rather than rejection of the entire SCC.
	g := fairFixture(3, 2)
	g.Enabled = []uint16{3, 1, 0}
	g.VictimOwns[2] = true
	g.Edges[0] = []fairEdge{{To: 1, Actor: 0, Progress: true}, {To: 2, Actor: 1, Progress: true}}
	g.Edges[1] = []fairEdge{{To: 0, Actor: 0, Progress: true}, {To: 1, Actor: 0, Progress: true}}
	r := fairRequire(t, g, true)
	if r.Pruned != 1 {
		t.Fatalf("must remove exactly the unfair enabling node, got %d", r.Pruned)
	}
	for _, n := range r.Cycle.Nodes {
		if n != 1 {
			t.Fatalf("hidden fair cycle must remain at node 1, got %v", r.Cycle.Nodes)
		}
	}
}

func TestFairCycleRecomputesFairnessAfterRepeatedPruning(t *testing.T) {
	g := fairFixture(4, 3)
	g.Enabled = []uint16{3, 5, 1, 0}
	g.VictimOwns[3] = true
	g.Edges[0] = []fairEdge{{To: 1, Actor: 0, Progress: true}, {To: 3, Actor: 1, Progress: true}}
	g.Edges[1] = []fairEdge{{To: 0, Actor: 0, Progress: true}, {To: 2, Actor: 0, Progress: true}, {To: 0, Actor: 2, Progress: true}}
	g.Edges[2] = []fairEdge{{To: 1, Actor: 0, Progress: true}, {To: 2, Actor: 0, Progress: true}}
	r := fairRequire(t, g, true)
	if r.Pruned != 2 {
		t.Fatalf("actor 2 loses its progress edge after pruning actor 1's enabling node; got %d pruned nodes", r.Pruned)
	}
	for _, node := range r.Cycle.Nodes {
		if node != 2 {
			t.Fatalf("only the twice-pruned inner SCC is fair: %v", r.Cycle.Nodes)
		}
	}
	g.Edges[2] = g.Edges[2][:1]
	fairRequire(t, g, false)
}

func TestFairCycleLongGraphAndWitness(t *testing.T) {
	const n = 20000
	g := fairFixture(n, 1)
	for i := range g.Edges {
		g.Enabled[i] = 1
		g.Edges[i] = []fairEdge{{To: uint32((i + 1) % n), Actor: 0, Progress: true}}
	}
	r := fairRequire(t, g, true)
	if len(r.Cycle.Edges) != n {
		t.Fatalf("the long directed ring's witness must traverse all %d edges, got %d", n, len(r.Cycle.Edges))
	}
}

func TestFairCycleRequiresIntermittentlyEnabledActor(t *testing.T) {
	g := fairFixture(3, 2)
	g.Enabled = []uint16{3, 1, 0}
	g.VictimOwns[2] = true
	g.Edges[0] = []fairEdge{{To: 1, Actor: 0, Progress: true}, {To: 2, Actor: 1, Progress: true}}
	g.Edges[1] = []fairEdge{{To: 0, Actor: 0, Progress: true}}
	fairRequire(t, g, false)
}

func TestFairCycleIncludesEachRequiredActor(t *testing.T) {
	g := fairFixture(2, 2)
	g.Enabled = []uint16{3, 3}
	g.Edges[0] = []fairEdge{{To: 1, Actor: 0, Progress: true}, {To: 0, Actor: 1, Progress: true}}
	g.Edges[1] = []fairEdge{{To: 0, Actor: 0, Progress: true}}
	r := fairRequire(t, g, true)
	var ran uint16
	for _, e := range r.Cycle.Edges {
		if e.Progress {
			ran |= 1 << uint(e.Actor)
		}
	}
	if ran != 3 {
		t.Fatalf("witness omitted a required actor: mask %02b", ran)
	}
}

func TestFairCycleAgeingUsesJustice(t *testing.T) {
	t.Run("continuously enabled threshold cannot be postponed", func(t *testing.T) {
		g := fairFixture(2, 2)
		g.Enabled = []uint16{2, 0}
		g.AgeMask = []uint16{1, 0}
		g.VictimAged[0] = false
		g.VictimOwns[1] = true
		g.Edges[0] = []fairEdge{{To: 0, Actor: 1, Progress: true}, {To: 1, Actor: 0, Age: true}}
		fairRequire(t, g, false)
	})
	t.Run("fresh traffic can finish before threshold", func(t *testing.T) {
		g := fairFixture(3, 2)
		g.Enabled = []uint16{2, 2, 2}
		g.AgeMask = []uint16{2, 0, 2}
		g.Edges[0] = []fairEdge{{To: 1, Actor: 1, Progress: true}}
		g.Edges[1] = []fairEdge{{To: 2, Actor: 1, Progress: true}}
		g.Edges[2] = []fairEdge{{To: 2, Actor: 1, Progress: true}, {To: 0, Actor: 1, Progress: true}}
		r := fairRequire(t, g, true)
		var visitedFreshCompletion bool
		for _, n := range r.Cycle.Nodes {
			visitedFreshCompletion = visitedFreshCompletion || n == 1
		}
		if !visitedFreshCompletion {
			t.Fatal("witness must visit the state disabling the fresh waiter's threshold")
		}
	})
	t.Run("age step does not satisfy actor progress", func(t *testing.T) {
		g := fairFixture(1, 1)
		g.Enabled[0], g.AgeMask[0] = 1, 1
		g.Edges[0] = []fairEdge{{To: 0, Actor: 0, Age: true}}
		fairRequire(t, g, false)
	})
	t.Run("continuously enabled ageing must execute", func(t *testing.T) {
		g := fairFixture(1, 2)
		g.Enabled[0], g.AgeMask[0] = 1, 2
		g.Edges[0] = []fairEdge{{To: 0, Actor: 0, Progress: true}}
		fairRequire(t, g, false)
		g.Edges[0] = append(g.Edges[0], fairEdge{To: 0, Actor: 1, Age: true})
		fairRequire(t, g, true)
	})
}

func TestFairCycleFiltersVictimAndRequiresActualCycle(t *testing.T) {
	for _, mode := range []string{"inactive", "owns", "unaged", "acyclic"} {
		t.Run(mode, func(t *testing.T) {
			g := fairFixture(1, 1)
			g.Enabled[0] = 1
			g.Edges[0] = []fairEdge{{To: 0, Actor: 0, Progress: true}}
			switch mode {
			case "inactive":
				g.VictimActive[0] = false
			case "owns":
				g.VictimOwns[0] = true
			case "unaged":
				g.VictimAged[0] = false
			case "acyclic":
				g.Enabled[0], g.Edges[0] = 0, nil
			}
			fairRequire(t, g, false)
		})
	}
	fairRequire(t, fairGraph{}, false)
}

func TestFairCycleMaskBoundaryAndStutter(t *testing.T) {
	g := fairFixture(1, fairMaxActors)
	g.Enabled[0], g.AgeMask[0] = 1<<15, 1<<15
	g.Edges[0] = []fairEdge{
		{To: 0, Actor: 15, Progress: true},
		{To: 0, Actor: 15, Age: true},
	}
	fairRequire(t, g, true)
	g = fairFixture(1, 0)
	g.Edges[0] = []fairEdge{{To: 0, Actor: -1}}
	fairRequire(t, g, true)
}

func TestFairCycleValidatesGraph(t *testing.T) {
	for _, mode := range []string{"shape", "actors", "enabled", "age-mask", "target", "progress-actor", "age-actor"} {
		t.Run(mode, func(t *testing.T) {
			g := fairFixture(1, 1)
			switch mode {
			case "shape":
				g.Enabled = nil
			case "actors":
				g.Actors = 17
			case "enabled":
				g.Enabled[0] = 2
			case "age-mask":
				g.AgeMask[0] = 2
			case "target":
				g.Edges[0] = []fairEdge{{To: 1, Actor: -1}}
			case "progress-actor":
				g.Edges[0] = []fairEdge{{To: 0, Actor: -1, Progress: true}}
			case "age-actor":
				g.Edges[0] = []fairEdge{{To: 0, Actor: 1, Age: true}}
			}
			r, err := findFairCycle(g)
			if err == nil || r.Exhaustive || r.Cycle != nil {
				t.Fatalf("malformed graph must be inconclusive: result=%+v err=%v", r, err)
			}
		})
	}
}

func TestFairCycleExhaustiveTwoVertexGraphs(t *testing.T) {
	// Every ordered pair either lacks an edge or has a stutter, progress,
	// age, or combined edge. Enumerate all graph and one-actor metadata
	// combinations, then compare with the independent recurrent-set oracle.
	for graphCode := 0; graphCode < 625; graphCode++ {
		base := fairFixture(2, 1)
		code := graphCode
		for from := 0; from < 2; from++ {
			for to := 0; to < 2; to++ {
				style := code % 5
				code /= 5
				if style != 0 {
					e := fairEdge{To: uint32(to), Actor: 0, Progress: style == 2 || style == 4, Age: style == 3 || style == 4}
					base.Edges[from] = append(base.Edges[from], e)
				}
			}
		}
		for flags := 0; flags < 64; flags++ {
			for node := 0; node < 2; node++ {
				base.Enabled[node] = uint16((flags >> uint(node)) & 1)
				base.AgeMask[node] = uint16((flags >> uint(node+2)) & 1)
				base.VictimAged[node] = flags&(1<<uint(node+4)) != 0
			}
			fairRequire(t, base, fairSubsetOracle(base))
		}
	}
}

func TestFairCycleMatchesIndependentSubsetOracle(t *testing.T) {
	// Enumerating recurrent vertex subsets is exponential but independent
	// of the production SCC pruning. Random finite graphs exercise nested
	// unfair SCCs, disconnected components, parallel edges, and age justice.
	rng := rand.New(rand.NewSource(8472301))
	for trial := 0; trial < 2000; trial++ {
		n, actors := 1+rng.Intn(7), 1+rng.Intn(3)
		g := fairFixture(n, actors)
		for i := range g.Edges {
			g.Enabled[i] = uint16(rng.Intn(1 << uint(actors)))
			g.AgeMask[i] = uint16(rng.Intn(1 << uint(actors)))
			g.VictimActive[i] = rng.Intn(5) != 0
			g.VictimOwns[i] = rng.Intn(5) == 0
			g.VictimAged[i] = rng.Intn(3) != 0
			for j := 0; j < n; j++ {
				for k := 0; k < rng.Intn(3); k++ {
					e := fairEdge{To: uint32(j), Actor: rng.Intn(actors)}
					switch rng.Intn(3) {
					case 0:
						e.Progress = true
					case 1:
						e.Age = true
					}
					g.Edges[i] = append(g.Edges[i], e)
				}
			}
		}
		want := fairSubsetOracle(g)
		t.Run(fmt.Sprint(trial), func(t *testing.T) { fairRequire(t, g, want) })
	}
}

func fairFixture(n, actors int) fairGraph {
	g := fairGraph{
		Edges: make([][]fairEdge, n), Enabled: make([]uint16, n),
		VictimActive: make([]bool, n), VictimOwns: make([]bool, n),
		VictimAged: make([]bool, n), Actors: actors, AgeMask: make([]uint16, n),
	}
	for i := range g.Edges {
		g.VictimActive[i], g.VictimAged[i] = true, true
	}
	return g
}

func fairRequire(t *testing.T, g fairGraph, want bool) fairResult {
	t.Helper()
	r, err := findFairCycle(g)
	if err != nil || !r.Exhaustive || (r.Cycle != nil) != want {
		t.Fatalf("fair cycle existence=%t: result=%+v err=%v graph=%+v", want, r, err, g)
	}
	if r.Cycle != nil {
		fairValidateWitness(t, g, *r.Cycle)
	}
	return r
}

func fairValidateWitness(t *testing.T, g fairGraph, c fairCycle) {
	t.Helper()
	if len(c.Edges) == 0 || len(c.Nodes) != len(c.Edges)+1 || c.Nodes[0] != c.Nodes[len(c.Nodes)-1] {
		t.Fatalf("witness must be a nonempty closed walk: %+v", c)
	}
	var enabled, progressed, ages uint16
	ageEnabled := uint16((uint32(1) << uint(g.Actors)) - 1)
	aged := false
	for i, e := range c.Edges {
		from, to := c.Nodes[i], c.Nodes[i+1]
		if int(from) >= len(g.Edges) || to != e.To {
			t.Fatalf("invalid witness edge %d: %+v", i, c)
		}
		found := false
		for _, actual := range g.Edges[from] {
			found = found || actual == e
		}
		if !found || !g.VictimActive[from] || g.VictimOwns[from] {
			t.Fatalf("edge leaves the continuously pending victim graph at %d: %+v", i, c)
		}
		enabled |= g.Enabled[from]
		ageEnabled &= g.AgeMask[from]
		aged = aged || g.VictimAged[from]
		if e.Progress {
			progressed |= 1 << uint(e.Actor)
		}
		if e.Age {
			ages |= 1 << uint(e.Actor)
		}
	}
	if !aged || enabled&^progressed != 0 || ageEnabled&^ages != 0 {
		t.Fatalf("witness is unfair: enabled=%b progress=%b age-enabled=%b age=%b victim-aged=%t cycle=%+v",
			enabled, progressed, ageEnabled, ages, aged, c)
	}
}

func fairSubsetOracle(g fairGraph) bool {
	n := len(g.Edges)
	for selected := 1; selected < 1<<uint(n); selected++ {
		var enabled, progress, age uint16
		ageEnabled := uint16((uint32(1) << uint(g.Actors)) - 1)
		aged, cyclic, eligible := false, false, true
		for i := 0; i < n; i++ {
			if selected&(1<<uint(i)) == 0 {
				continue
			}
			if !g.VictimActive[i] || g.VictimOwns[i] {
				eligible = false
				break
			}
			enabled |= g.Enabled[i]
			ageEnabled &= g.AgeMask[i]
			aged = aged || g.VictimAged[i]
			for _, e := range g.Edges[i] {
				if selected&(1<<uint(e.To)) == 0 {
					continue
				}
				cyclic = true
				if e.Progress {
					progress |= 1 << uint(e.Actor)
				}
				if e.Age {
					age |= 1 << uint(e.Actor)
				}
			}
		}
		if !eligible || !aged || !cyclic || enabled&^progress != 0 || ageEnabled&^age != 0 {
			continue
		}
		strong := true
		for start := 0; start < n && strong; start++ {
			if selected&(1<<uint(start)) == 0 {
				continue
			}
			reached := 1 << uint(start)
			for changed := true; changed; {
				changed = false
				for from := 0; from < n; from++ {
					if reached&(1<<uint(from)) == 0 {
						continue
					}
					for _, e := range g.Edges[from] {
						bit := 1 << uint(e.To)
						if selected&bit != 0 && reached&bit == 0 {
							reached |= bit
							changed = true
						}
					}
				}
			}
			strong = reached == selected
		}
		if strong {
			return true
		}
	}
	return false
}
