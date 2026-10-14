// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import "fmt"

const fairMaxActors = 16

// fairEdge identifies a graph transition. Progress records an actual step by
// Actor; Age records the threshold process for Actor. An ageing transition
// alone does not discharge the obligation to schedule that actor.
type fairEdge struct {
	To       uint32
	Actor    int
	Progress bool
	Age      bool
}

// fairGraph must contain a complete, reachable finite state graph. Enabled
// marks actors with an enabled progress operation. AgeMask marks threshold
// processes that are enabled because their actor has started waiting and has
// not yet aged. An actor finishing one operation and starting another must
// pass through a state where its previous threshold process is disabled.
//
// VictimActive means that the same uncancelled victim operation is pending.
// VictimAged marks eligibility for cycle acceptance. For a registered waiter,
// it is the actual monotone threshold flag. Before first registration, set it
// true: no threshold process is enabled yet, so a pre-registration livelock
// must be checked without requiring a nonexistent age event. First registration
// cannot be revisited by that operation; within a recurrent registered-waiter
// component the actual age flag must not reset. Acquisition or cancellation
// ends the victim operation and leaves the subgraph. Graph construction,
// operation identity, reachability, and completeness are the caller's duties.
type fairGraph struct {
	Edges        [][]fairEdge
	Enabled      []uint16
	VictimActive []bool
	VictimOwns   []bool
	VictimAged   []bool
	Actors       int
	AgeMask      []uint16
}

// fairCycle is a nonempty closed walk: Nodes has one more entry than Edges,
// Nodes[0] == Nodes[len(Nodes)-1], and Edges[i].To == Nodes[i+1]. Repeating
// this walk gives an infinite execution satisfying the stated fairness.
type fairCycle struct {
	Nodes []uint32
	Edges []fairEdge
}

// Exhaustive describes the analysis of the supplied graph, not whether its
// producer finished exploring the underlying model. A cycle is a complete
// existential counterexample; otherwise every possible recurrent component
// has been excluded. Invalid graph inputs leave Exhaustive false.
type fairResult struct {
	Cycle      *fairCycle
	Exhaustive bool
	Components int
	Pruned     int
}

// findFairCycle finds starvation under strong actor fairness and weak time
// fairness (justice). Strong fairness requires an actor enabled infinitely
// often to take progress steps infinitely often. Justice requires a threshold
// process enabled continuously to take steps; fresh waits may always finish
// before their threshold is reached.
//
// For any infinite execution in a finite graph, the states visited infinitely
// often form a strongly connected set. In a candidate SCC, strong fairness can
// be met when every actor enabled at any vertex has an internal progress edge.
// If an actor has no such edge, any fair recurrent subset must omit every
// vertex enabling it. Remove those vertices and recompute SCCs; rejecting the
// whole original SCC would miss fair subsets. Each pruning round removes an
// actor's enabling vertices permanently within that branch, so a vertex can
// survive at most Actors pruning rounds.
//
// A threshold process enabled at every vertex but lacking an internal age edge
// makes the entire SCC impossible: every subset has the same unmet justice
// obligation. Otherwise a closed walk can either visit a vertex disabling the
// process or include its age edge. The witness visits an eligible victim
// vertex: a registered continuous wait must cross the threshold, while a
// pre-registration component has no threshold process to schedule.
func findFairCycle(g fairGraph) (fairResult, error) {
	if err := validateFairGraph(g); err != nil {
		return fairResult{}, err
	}
	s := newFairSearch(g)
	vertices := make([]uint32, 0, len(g.Edges))
	for node := range g.Edges {
		if g.VictimActive[node] && !g.VictimOwns[node] {
			vertices = append(vertices, uint32(node))
		}
	}
	pending := s.components(vertices)
	r := fairResult{}
	for len(pending) != 0 {
		last := len(pending) - 1
		component := pending[last]
		pending = pending[:last]
		r.Components++
		info := s.assess(component)
		if !info.cyclic || !info.hasAged || info.ageEnabled&^info.ageProgress != 0 {
			continue
		}
		missing := info.enabled &^ info.progress
		if missing != 0 {
			survivors := make([]uint32, 0, len(component))
			for _, node := range component {
				if g.Enabled[node]&missing == 0 {
					survivors = append(survivors, node)
				} else {
					r.Pruned++
				}
			}
			pending = append(pending, s.components(survivors)...)
			continue
		}
		cycle, err := s.witness(component, info)
		if err != nil {
			return r, err
		}
		r.Cycle, r.Exhaustive = cycle, true
		return r, nil
	}
	r.Exhaustive = true
	return r, nil
}

func validateFairGraph(g fairGraph) error {
	n := len(g.Edges)
	if g.Actors < 0 || g.Actors > fairMaxActors {
		return fmt.Errorf("fair graph has %d actors; masks support 0..%d", g.Actors, fairMaxActors)
	}
	if uint64(n) > uint64(^uint32(0)) {
		return fmt.Errorf("fair graph has too many vertices: %d", n)
	}
	if len(g.Enabled) != n || len(g.AgeMask) != n || len(g.VictimActive) != n || len(g.VictimOwns) != n || len(g.VictimAged) != n {
		return fmt.Errorf("fair graph metadata lengths differ from its %d vertices", n)
	}
	mask := fairActorMask(g.Actors)
	for node, edges := range g.Edges {
		if g.Enabled[node]&^mask != 0 || g.AgeMask[node]&^mask != 0 {
			return fmt.Errorf("fair graph vertex %d enables an actor outside 0..%d", node, g.Actors-1)
		}
		if uint64(len(edges)) > uint64(^uint32(0)) {
			return fmt.Errorf("fair graph vertex %d has too many edges", node)
		}
		for _, e := range edges {
			if uint64(e.To) >= uint64(n) {
				return fmt.Errorf("fair graph edge from %d has invalid target %d", node, e.To)
			}
			if (e.Progress || e.Age) && (e.Actor < 0 || e.Actor >= g.Actors) {
				return fmt.Errorf("fair graph edge from %d has invalid actor %d", node, e.Actor)
			}
		}
	}
	return nil
}

func fairActorMask(actors int) uint16 {
	return uint16((uint32(1) << uint(actors)) - 1)
}

type fairSearch struct {
	graph   fairGraph
	member  []bool
	index   []uint32
	low     []uint32
	stacked []bool
}

func newFairSearch(g fairGraph) *fairSearch {
	n := len(g.Edges)
	return &fairSearch{
		graph: g, member: make([]bool, n), index: make([]uint32, n),
		low: make([]uint32, n), stacked: make([]bool, n),
	}
}

// components is iterative Tarjan on the given induced graph. Explicit DFS
// frames avoid a call stack proportional to the size of a model's graph.
func (s *fairSearch) components(vertices []uint32) [][]uint32 {
	for _, node := range vertices {
		s.member[node], s.index[node] = true, 0
	}
	type frame struct {
		node uint32
		next int
	}
	var components [][]uint32
	var stack []uint32
	var dfs []frame
	var serial uint32
	visit := func(node uint32) {
		serial++
		s.index[node], s.low[node], s.stacked[node] = serial, serial, true
		stack = append(stack, node)
		dfs = append(dfs, frame{node: node})
	}
	for _, start := range vertices {
		if s.index[start] != 0 {
			continue
		}
		visit(start)
		for len(dfs) != 0 {
			f := &dfs[len(dfs)-1]
			node := f.node
			if f.next < len(s.graph.Edges[node]) {
				to := s.graph.Edges[node][f.next].To
				f.next++
				if !s.member[to] {
					continue
				}
				if s.index[to] == 0 {
					visit(to)
				} else if s.stacked[to] && s.index[to] < s.low[node] {
					s.low[node] = s.index[to]
				}
				continue
			}
			if s.low[node] == s.index[node] {
				var component []uint32
				for {
					last := len(stack) - 1
					popped := stack[last]
					stack = stack[:last]
					s.stacked[popped] = false
					component = append(component, popped)
					if popped == node {
						break
					}
				}
				components = append(components, component)
			}
			dfs = dfs[:len(dfs)-1]
			if len(dfs) != 0 {
				parent := dfs[len(dfs)-1].node
				if s.low[node] < s.low[parent] {
					s.low[parent] = s.low[node]
				}
			}
		}
	}
	for _, node := range vertices {
		s.member[node] = false
	}
	return components
}

type fairStep struct {
	from uint32
	edge fairEdge
}

type fairComponent struct {
	enabled, progress       uint16
	ageEnabled, ageProgress uint16
	cyclic, hasAged         bool
	aged                    uint32
	progressStep            [fairMaxActors]fairStep
	ageStep                 [fairMaxActors]fairStep
	ageDisabled             [fairMaxActors]uint32
}

func (s *fairSearch) assess(component []uint32) fairComponent {
	info := fairComponent{cyclic: len(component) > 1, ageEnabled: fairActorMask(s.graph.Actors)}
	for _, node := range component {
		s.member[node] = true
	}
	var ageDisabled uint16
	for _, node := range component {
		info.enabled |= s.graph.Enabled[node]
		info.ageEnabled &= s.graph.AgeMask[node]
		if s.graph.VictimAged[node] && !info.hasAged {
			info.aged, info.hasAged = node, true
		}
		for actor := 0; actor < s.graph.Actors; actor++ {
			bit := uint16(1) << uint(actor)
			if s.graph.AgeMask[node]&bit == 0 && ageDisabled&bit == 0 {
				info.ageDisabled[actor] = node
				ageDisabled |= bit
			}
		}
		for _, e := range s.graph.Edges[node] {
			if !s.member[e.To] {
				continue
			}
			info.cyclic = info.cyclic || e.To == node
			if e.Progress {
				bit := uint16(1) << uint(e.Actor)
				if info.progress&bit == 0 {
					info.progressStep[e.Actor] = fairStep{from: node, edge: e}
					info.progress |= bit
				}
			}
			if e.Age {
				bit := uint16(1) << uint(e.Actor)
				if info.ageProgress&bit == 0 {
					info.ageStep[e.Actor] = fairStep{from: node, edge: e}
					info.ageProgress |= bit
				}
			}
		}
	}
	for _, node := range component {
		s.member[node] = false
	}
	return info
}

// witness joins at most Actors progress edges, Actors threshold edges or
// disabling states, and an aged-victim state. It need not tour every vertex:
// the progress edges cover every actor any connecting path could enable, and
// each time process is either disabled on the walk or explicitly scheduled.
func (s *fairSearch) witness(component []uint32, info fairComponent) (*fairCycle, error) {
	for _, node := range component {
		s.member[node] = true
	}
	defer func() {
		for _, node := range component {
			s.member[node] = false
		}
	}()
	b := fairWalk{search: s, cycle: fairCycle{Nodes: []uint32{info.aged}}, ageEnabled: s.graph.AgeMask[info.aged]}
	for actor := 0; actor < s.graph.Actors; actor++ {
		if info.enabled&(uint16(1)<<uint(actor)) != 0 {
			if err := b.step(info.progressStep[actor]); err != nil {
				return nil, err
			}
		}
	}
	for actor := 0; actor < s.graph.Actors; actor++ {
		bit := uint16(1) << uint(actor)
		if b.ageEnabled&bit == 0 {
			continue
		}
		if info.ageEnabled&bit == 0 {
			if err := b.path(info.ageDisabled[actor]); err != nil {
				return nil, err
			}
		} else if err := b.step(info.ageStep[actor]); err != nil {
			return nil, err
		}
	}
	if err := b.path(info.aged); err != nil {
		return nil, err
	}
	if len(b.cycle.Edges) == 0 {
		// With no fairness obligations the start can stand alone only if
		// it has a self-loop; otherwise leave and return within its SCC.
		for _, e := range s.graph.Edges[info.aged] {
			if s.member[e.To] {
				b.append(e)
				if err := b.path(info.aged); err != nil {
					return nil, err
				}
				break
			}
		}
	}
	return &b.cycle, nil
}

type fairWalk struct {
	search     *fairSearch
	cycle      fairCycle
	ageEnabled uint16
	queue      []uint32
	reverse    []fairEdge
}

func (b *fairWalk) append(e fairEdge) {
	b.cycle.Edges = append(b.cycle.Edges, e)
	b.cycle.Nodes = append(b.cycle.Nodes, e.To)
	b.ageEnabled &= b.search.graph.AgeMask[e.To]
}

func (b *fairWalk) step(step fairStep) error {
	if err := b.path(step.from); err != nil {
		return err
	}
	b.append(step.edge)
	return nil
}

// path appends a shortest internal path from the walk's end to its target.
// Tarjan's scratch arrays can be reused now: stacked is the BFS visited set,
// low stores predecessors, and index stores the predecessor's edge number.
func (b *fairWalk) path(target uint32) error {
	start := b.cycle.Nodes[len(b.cycle.Nodes)-1]
	if start == target {
		return nil
	}
	s := b.search
	b.queue = append(b.queue[:0], start)
	s.stacked[start] = true
	found := false
	for head := 0; head < len(b.queue) && !found; head++ {
		from := b.queue[head]
		for edgeIndex, e := range s.graph.Edges[from] {
			if !s.member[e.To] || s.stacked[e.To] {
				continue
			}
			s.stacked[e.To], s.low[e.To], s.index[e.To] = true, from, uint32(edgeIndex)
			b.queue = append(b.queue, e.To)
			if e.To == target {
				found = true
				break
			}
		}
	}
	for _, node := range b.queue {
		s.stacked[node] = false
	}
	if !found {
		return fmt.Errorf("fair SCC witness has no path from %d to %d", start, target)
	}
	b.reverse = b.reverse[:0]
	for node := target; node != start; {
		from := s.low[node]
		b.reverse = append(b.reverse, s.graph.Edges[from][s.index[node]])
		node = from
	}
	for i := len(b.reverse) - 1; i >= 0; i-- {
		b.append(b.reverse[i])
	}
	return nil
}
