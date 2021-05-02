#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

# enable computation time
sed -i '/^buffer_size/a computation_speed = 0.1' *.toml

configs=(
standard2.toml
standard3.toml
background_dynamic_strong.toml
background_off.toml
background_static_strong.toml
level2bad.toml
)

for conf in ${configs[@]}; do
	echo $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
done

wait
