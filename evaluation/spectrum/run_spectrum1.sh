#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

background_flow_freqs=(
25_000_000
50_000_000
1_00_000_000
2_00_000_000
4_00_000_000
8_00_000_000
1_600_000_000
3_200_000_000
6_400_000_000
12_800_000_000
25_600_000_000
51_200_000_000
102_400_000_000
)

cnt=0
for f in ${background_flow_freqs[@]}; do
	echo $f
	conf=spectrum1_$f.toml
	cp spectrum1_base.toml $conf
	sed -i "s/\(.*\)frequency_ns = 2_00_000_000\(.*\)/\1frequency_ns = $f\2/" $conf
	sed -i "s/spectrum1_1/spectrum1_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
