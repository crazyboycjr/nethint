#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

# enable computation
# sed -i '/^buffer_size/a computation_speed = 0.1' *.toml

configs=(
# standard3.toml
# standard3_pervm.toml
# standard3_pertenant.toml
mccs1.toml
)

for conf in ${configs[@]}; do
	echo $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
done

wait
