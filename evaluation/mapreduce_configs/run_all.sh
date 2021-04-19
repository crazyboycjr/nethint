#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

for conf in `ls *.toml`; do
	echo $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c $conf &
done

# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c standard_hybrid1.toml &
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c standard_hybrid2.toml &
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c standard_hybrid3.toml &

wait
