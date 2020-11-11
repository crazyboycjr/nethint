#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

auto_tunes=(
1
2
4
8
16
32
64
128
)

cnt=0
for f in ${auto_tunes[@]}; do
	echo $f
	conf=spectrum4_$f.toml
	cp spectrum4_base.toml $conf
	sed -i "s/^auto_tune = 10/auto_tune = $f/" $conf
	sed -i "s/spectrum4_1/spectrum4_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
