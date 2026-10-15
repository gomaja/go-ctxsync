#!/bin/bash
# Interleaved base / final32 (archived candidate) / final64 comparison.
CPUS=$1 COUNT=$2 BT=$3 TAG=$4
cd "$(dirname "$0")"
{ date -u +%FT%TZ; go version; uptime; printf "affinity: %s\ncount: %s\nbenchtime: %s\n" "$CPUS" "$COUNT" "$BT"; sha256sum base.test final32.test final64.test; } > env-$TAG.txt
for v in base final32 final64; do : > $v-$TAG.txt; done
for i in $(seq 1 "$COUNT"); do
  for v in base final32 final64; do
    taskset -c "$CPUS" ./$v.test -test.run '^$' -test.bench '^BenchmarkRWParallel$/.*/.*/^impl=ctxsync$' -test.benchtime "$BT" -test.count 1 -test.cpu 4,16 >> $v-$TAG.txt
    taskset -c "$CPUS" ./$v.test -test.run '^$' -test.bench '^BenchmarkMutexParallel$/.*/^impl=ctxsync$' -test.benchtime "$BT" -test.count 1 -test.cpu 4,16 >> $v-$TAG.txt
  done
done
uptime >> env-$TAG.txt
~/go/bin/benchstat base-$TAG.txt final64-$TAG.txt > cmp-$TAG-base-final64.txt 2>&1
~/go/bin/benchstat final32-$TAG.txt final64-$TAG.txt > cmp-$TAG-final32-final64.txt 2>&1
echo DONE > done-$TAG.txt
