#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

for conf in `ls *.toml`; do
	echo $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c $conf &
done

wait
