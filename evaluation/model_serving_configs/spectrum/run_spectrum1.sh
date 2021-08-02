#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

background_flow_freqs=(
1_00_000_000
2_00_000_000
4_00_000_000
8_00_000_000
1_600_000_000
3_200_000_000
6_400_000_000
12_800_000_000
)

for f in ${background_flow_freqs[@]}; do
	echo $f
	conf=spectrum1_$f.toml
	cp spectrum1_base.toml $conf
	sed -i "s/\(.*\)frequency_ns = 2_00_000_000\(.*\)/\1frequency_ns = $f\2/" $conf
	sed -i "s/spectrum1_1/spectrum1_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin rl_experiment --release -- -P 5 -c $conf &
done

wait
