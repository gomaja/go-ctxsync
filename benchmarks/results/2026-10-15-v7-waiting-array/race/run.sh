#!/bin/bash
# Saved race-mode evidence for issue #1: instrumented guard timings and the issue's reproducer.
# Base = de26fb4, final = the fix branch. CPUs 17-31 (15 CPUs), GOMAXPROCS=32, as in the issue.
set -u
cd "$(dirname "$0")"
I=<instrumented copies>
{ date -u +%FT%TZ; go version; uptime; sha256sum base.race.test final.race.test $I/instrx.race.test $I/instrt.race.test repro_test.go; } > environment.txt
for v in x t; do
  for i in 1 2 3; do
    GOMAXPROCS=32 GORACE=atexit_sleep_ms=0 taskset -c 17-31 timeout 300 $I/instr$v.race.test -test.run Instrumented -test.v > instrumented-$([ $v = x ] && echo base || echo final)-$i.log 2>&1
  done
done
for v in base final; do
  : > reproducer-$v.txt
  for i in $(seq 1 10); do
    s=$(date +%s.%N)
    GOMAXPROCS=32 timeout 180 taskset -c 17-31 ./$v.race.test -test.run '^TestContendedCancellation$' -test.count=1 > reproducer-$v-$i.log 2>&1
    rc=$?
    printf '%s trial=%d rc=%d wall=%.2fs %s\n' $v $i $rc "$(echo "$(date +%s.%N) - $s" | bc)" "$(grep -m1 -o 'only [0-9]*/[0-9]* cancellable acquisitions returned' reproducer-$v-$i.log)" >> reproducer-$v.txt
  done
done
uptime >> environment.txt
echo DONE > done.txt
