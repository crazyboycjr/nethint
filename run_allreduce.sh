#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

mkdir -p results

for nethint in 0 1;
do
  for jobs in 4 8 16; do
    for test_index in 0; do
      RUST_LOG=info cargo run -- -n $jobs -h $nethint arbitrary 150 8 100 200 > results/nethint-$nethint-jobs-$jobs-$test_index.txt &
    done
  done
done