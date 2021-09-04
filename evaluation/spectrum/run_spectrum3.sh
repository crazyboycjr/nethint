#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

auto_tunes=(
10
20
40
80
160
320
640
1280
)

cnt=0
for f in ${auto_tunes[@]}; do
	echo $f
	conf=spectrum3_$f.toml
	cp spectrum3_base.toml $conf
	sed -i "s/^auto_tune = 10/auto_tune = $f/" $conf
	sed -i "s/spectrum3_1/spectrum3_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin rl_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
