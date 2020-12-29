#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

mkdir -p results

for nethint in 0 1;
do
  for cjobs in 4 8 16; do
    for test_index in 0 1 2 3 4; do
      RUST_LOG=info cargo run --release -- -n $cjobs -l $nethint arbitrary 150 8 100 200 2>&1 > results/nethint-$nethint-jobs-$cjobs-$test_index.txt &
    done
  done
done

sleep 20m