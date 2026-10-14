#!/usr/bin/env bash
# Copyright 2026 gomaja. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Run on an otherwise idle machine. The first argument is the benchmark
# duration (default 1s); every case still receives ten samples. Set
# CTXSYNC_BENCH_CPUS to a taskset CPU list (for example 8-23) to pin the runs
# to quiet CPUs; the published results were measured pinned to 8-23.
ctxsync_bench_duration=${1:-1s}
ctxsync_bench_cpus=${CTXSYNC_BENCH_CPUS:-}
ctxsync_pin=()
if [ -n "$ctxsync_bench_cpus" ]; then
  ctxsync_pin=(taskset -c "$ctxsync_bench_cpus")
fi
ctxsync_benchstat=$(command -v benchstat || printf '%s/bin/benchstat' "$(go env GOPATH)")
ctxsync_bench_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
ctxsync_bench_results="$ctxsync_bench_root/.scratch/benchmarks-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$ctxsync_bench_results"
cd "$ctxsync_bench_root/benchmarks"

{
  date -u +%Y-%m-%dT%H:%M:%SZ
  go version
  go env GOOS GOARCH GOAMD64 GOVERSION
  lscpu
  git rev-parse HEAD
  git status --short
  sha256sum ../guard.go ../mutex.go ../rwmutex.go ../go.mod lock_test.go go.mod go.sum
  printf 'affinity: %s\nsamples: 10\nduration: %s\nuncontended GOMAXPROCS: 1\nparallel GOMAXPROCS: 4,16\n' "${ctxsync_bench_cpus:-unpinned}" "$ctxsync_bench_duration"
} > "$ctxsync_bench_results/environment.txt"

${ctxsync_pin[@]+"${ctxsync_pin[@]}"} go test -c -o "$ctxsync_bench_results/locks.test" .
sha256sum "$ctxsync_bench_results/locks.test" > "$ctxsync_bench_results/binary.sha256"

${ctxsync_pin[@]+"${ctxsync_pin[@]}"} "$ctxsync_bench_results/locks.test" \
  -test.run '^$' -test.bench '^BenchmarkMutex(DirectLockUnlock|Uncontended)$' \
  -test.benchmem -test.benchtime "$ctxsync_bench_duration" -test.count 10 \
  -test.cpu 1 -test.timeout 30m > "$ctxsync_bench_results/uncontended.txt"

${ctxsync_pin[@]+"${ctxsync_pin[@]}"} "$ctxsync_bench_results/locks.test" \
  -test.run '^$' -test.bench '^Benchmark(MutexParallel|RWParallel)$' \
  -test.benchmem -test.benchtime "$ctxsync_bench_duration" -test.count 10 \
  -test.cpu 4,16 -test.timeout 30m > "$ctxsync_bench_results/parallel.txt"

"$ctxsync_benchstat" -col '/impl@(sync ctxsync semaphore retry retry-serialized)' \
  -row .fullname "$ctxsync_bench_results/uncontended.txt" > "$ctxsync_bench_results/uncontended-benchstat.txt"
"$ctxsync_benchstat" -col '/impl@(sync ctxsync semaphore retry retry-serialized)' \
  -row .fullname "$ctxsync_bench_results/parallel.txt" > "$ctxsync_bench_results/parallel-benchstat.txt"

printf 'Benchmark evidence: %s\n' "$ctxsync_bench_results"
