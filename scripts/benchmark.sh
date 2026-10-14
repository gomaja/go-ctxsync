#!/usr/bin/env bash
# Copyright 2026 gomaja. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Run only after other work on CPUs 8-23 has finished. The first argument is
# benchmark duration (default 1s); every case still receives ten samples.
ctxsync_bench_duration=${1:-1s}
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
  printf 'affinity: 8-23\nsamples: 10\nduration: %s\nuncontended GOMAXPROCS: 1\nparallel GOMAXPROCS: 4,16\n' "$ctxsync_bench_duration"
} > "$ctxsync_bench_results/environment.txt"

taskset -c 8-23 go test -c -o "$ctxsync_bench_results/locks.test" .
sha256sum "$ctxsync_bench_results/locks.test" > "$ctxsync_bench_results/binary.sha256"

taskset -c 8-23 "$ctxsync_bench_results/locks.test" \
  -test.run '^$' -test.bench '^BenchmarkMutex(DirectLockUnlock|Uncontended)$' \
  -test.benchmem -test.benchtime "$ctxsync_bench_duration" -test.count 10 \
  -test.cpu 1 -test.timeout 30m > "$ctxsync_bench_results/uncontended.txt"

taskset -c 8-23 "$ctxsync_bench_results/locks.test" \
  -test.run '^$' -test.bench '^Benchmark(MutexParallel|RWParallel)$' \
  -test.benchmem -test.benchtime "$ctxsync_bench_duration" -test.count 10 \
  -test.cpu 4,16 -test.timeout 30m > "$ctxsync_bench_results/parallel.txt"

"$HOME/go/bin/benchstat" -col '/impl@(sync ctxsync semaphore retry retry-serialized)' \
  -row .fullname "$ctxsync_bench_results/uncontended.txt" > "$ctxsync_bench_results/uncontended-benchstat.txt"
"$HOME/go/bin/benchstat" -col '/impl@(sync ctxsync semaphore retry retry-serialized)' \
  -row .fullname "$ctxsync_bench_results/parallel.txt" > "$ctxsync_bench_results/parallel-benchstat.txt"

printf 'Benchmark evidence: %s\n' "$ctxsync_bench_results"
