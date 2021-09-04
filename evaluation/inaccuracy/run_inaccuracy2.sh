#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

inaccuracies=(
0.0
0.1
0.2
0.3
0.4
0.5
0.6
0.7
0.8
0.9
)

cnt=0
for f in ${inaccuracies[@]}; do
	echo $f
	conf=inaccuracy2_$f.toml
	cp inaccuracy2_base.toml $conf
	sed -i "s/^inaccuracy = 0.1/inaccuracy = $f/" $conf
	sed -i "s/inaccuracy2_base/inaccuracy2_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin rl_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
