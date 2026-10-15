#!/bin/bash
# Final-code (64-bit slots) race evidence: instrumented timings and the issue reproducer,
# GOMAXPROCS=32 on 15 CPUs (0-14; CPUs 16-31 carry a concurrent throughput run).
set -u
cd "$(dirname "$0")"
I=<instrumented copies>
{ date -u +%FT%TZ; go version; uptime; sha256sum final64.race.test $I/instrt64.race.test repro_test.go; } > environment64.txt
for i in 1 2 3; do
  GOMAXPROCS=32 GORACE=atexit_sleep_ms=0 taskset -c 0-14 timeout 300 $I/instrt64.race.test -test.run Instrumented -test.v > instrumented-final64-$i.log 2>&1
done
: > reproducer-final64.txt
for i in $(seq 1 10); do
  s=$(date +%s.%N)
  GOMAXPROCS=32 timeout 180 taskset -c 0-14 ./final64.race.test -test.run '^TestContendedCancellation$' -test.count=1 > reproducer-final64-$i.log 2>&1
  rc=$?
  printf 'final64 trial=%d rc=%d wall=%.2fs %s\n' $i $rc "$(echo "$(date +%s.%N) - $s" | bc)" "$(grep -m1 -o 'only [0-9]*/[0-9]* cancellable acquisitions returned' reproducer-final64-$i.log)" >> reproducer-final64.txt
done
uptime >> environment64.txt
echo DONE > done64.txt
