#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

# enable computation time
# sed -i '/^num_reduce/a enable_computation_time = true' *.toml

configs=(
standard_hybrid2.toml
standard_hybrid4.toml
background_dynamic_strong.toml
background_off.toml
background_static_strong.toml
probe_bad.toml
)

for conf in ${configs[@]}; do
	echo $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -P 5 -c $conf &
done

wait
