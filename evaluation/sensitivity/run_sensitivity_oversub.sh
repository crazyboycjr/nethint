#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

rack_size=6
oversubs=(
1
1.5
2
3
4
10
)

cnt=0
for f in ${oversubs[@]}; do
	echo $f
	conf=sensitivity_oversub_$f.toml
	cp sensitivity_oversub_base.toml $conf
	rack_bw=`python3 -c "print('{:.0f}'.format(${rack_size} * 100 / ${f}))"`
	echo rack_bw: ${rack_bw}
	sed -i "s/^rack_bw = 0/rack_bw = ${rack_bw}/" $conf
	sed -i "s/sensitivity_oversub_base/sensitivity_oversub_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
