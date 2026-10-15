#!/bin/bash
# Interleaved base / final (stride 1) / stride-17 comparison on contended RW cases.
CPUS=$1 COUNT=$2 BT=$3 TAG=$4
cd "$(dirname "$0")"
for v in base final stride; do : > $v-$TAG.txt; done
for i in $(seq 1 "$COUNT"); do
  for v in base final stride; do
    taskset -c "$CPUS" ./$v.test -test.run '^$' -test.bench '^BenchmarkRWParallel$/.*/.*/^impl=ctxsync$' -test.benchtime "$BT" -test.count 1 -test.cpu 4,16 >> $v-$TAG.txt
  done
done
~/go/bin/benchstat base-$TAG.txt final-$TAG.txt > cmp-$TAG-final.txt 2>&1
~/go/bin/benchstat base-$TAG.txt stride-$TAG.txt > cmp-$TAG-stride.txt 2>&1
~/go/bin/benchstat final-$TAG.txt stride-$TAG.txt > cmp-$TAG-final-vs-stride.txt 2>&1
