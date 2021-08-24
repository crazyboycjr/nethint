#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

inaccuracies=(
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
	conf=inaccuracy_$f.toml
	cp inaccuracy_base.toml $conf
	sed -i "s/^inaccuracy = 0.1/inaccuracy = $f/" $conf
	sed -i "s/inaccuracy_base/inaccuracy_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
