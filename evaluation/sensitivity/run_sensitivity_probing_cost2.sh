#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -P 5 -c sensitivity_probing_cost2_baseline.toml &

round_mses=(
1
10
25
50
75
100
)

cnt=5
for f in ${round_mses[@]}; do
	echo round_ms: $f
	conf=sensitivity_probing_cost2_$f.toml
	cp sensitivity_probing_cost2_base.toml $conf
	sed -i "s/round_ms = 100/round_ms = $f/" $conf
	sed -i "s/sensitivity_probing_cost2_base/sensitivity_probing_cost2_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
