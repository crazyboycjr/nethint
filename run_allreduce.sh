#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

mkdir -p results

for nethint in 0 1;
do
  for cjobs in 1 2 4 8 16 32; do
    for workers in 4 8 12 16 20 24 28 32; do
    for test_index in 0 1 2 3 4 5 6 7 8 9; do
      RUST_LOG=info cargo run --release -- -n $cjobs -w $workers -l $nethint arbitrary 150 8 100 200 2>&1 > results/nethint-$nethint-jobs-$cjobs-workers-$workers-$test_index.txt &
    done
    done
  done
done

sleep 20m
