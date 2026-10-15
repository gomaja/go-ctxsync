#!/bin/bash
# Interleaved base/fix comparison: ten one-second samples per case, alternating binaries.
set -u
CPUS=${CPUS:-16-31}
cd "$(dirname "$0")"
{
  date -u +%Y-%m-%dT%H:%M:%SZ
  go version
  printf 'affinity: %s\nsamples: 10 (alternating base, fix)\nduration: 1s\nuncontended GOMAXPROCS: 1\nparallel GOMAXPROCS: 4,16\n' "$CPUS"
  printf 'base: %s\nfix: %s\n' "$(git -C <base checkout> rev-parse HEAD)" "$(git -C <fix checkout> rev-parse HEAD) + working tree"
  sha256sum <base checkout>/guard.go <fix checkout>/guard.go
  sha256sum base.test fix.test
  uptime
} > environment.txt
: > base.txt; : > fix.txt
for i in $(seq 1 10); do
  for v in base fix; do
    taskset -c "$CPUS" ./$v.test -test.run '^$' -test.bench '^BenchmarkMutex(DirectLockUnlock|Uncontended)$' -test.benchmem -test.benchtime 1s -test.count 1 -test.cpu 1 >> $v.txt
    taskset -c "$CPUS" ./$v.test -test.run '^$' -test.bench '^BenchmarkMutexParallel$/.*/^impl=(sync|ctxsync)$' -test.benchmem -test.benchtime 1s -test.count 1 -test.cpu 4,16 >> $v.txt
    taskset -c "$CPUS" ./$v.test -test.run '^$' -test.bench '^BenchmarkRWParallel$/.*/.*/^impl=(sync|ctxsync)$' -test.benchmem -test.benchtime 1s -test.count 1 -test.cpu 4,16 >> $v.txt
  done
  echo "sample $i done $(date -u +%T) load: $(cut -d' ' -f1-3 /proc/loadavg)" >> progress.log
done
uptime >> environment.txt
~/go/bin/benchstat base.txt fix.txt > benchstat.txt 2>&1
echo DONE >> progress.log
