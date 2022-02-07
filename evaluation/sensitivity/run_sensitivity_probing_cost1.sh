#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c sensitivity_probing_cost1_baseline.toml &

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
	conf=sensitivity_probing_cost1_$f.toml
	cp sensitivity_probing_cost1_base.toml $conf
	auto_tune=`python3 -c "print('{:.0f}'.format(${f} * 10))"`
	echo auto_tune: $auto_tune
	sed -i "s/round_ms = 100/round_ms = $f/" $conf
	sed -i "s/auto_tune = 1000/auto_tune = ${auto_tune}/" $conf
	sed -i "s/sensitivity_probing_cost1_base/sensitivity_probing_cost1_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
