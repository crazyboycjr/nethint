#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

configs=(
standard2.toml
background_dynamic_strong.toml
background_off.toml
background_static_strong.toml
level2bad.toml
)

for conf in ${configs[@]}; do
	echo $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin rl_experiment --release -- -P 5 -c $conf &
done

wait
