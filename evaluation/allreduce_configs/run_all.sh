#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

# enable computation time
sed -i '/^buffer_size/a computation_speed = 0.1' *.toml

for conf in `ls *.toml`; do
	echo $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 2 -c $conf &
done

wait
