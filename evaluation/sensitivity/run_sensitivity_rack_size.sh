#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

oversub=3
rack_sizes=(
5
10
20
40
)

cnt=0
for f in ${rack_sizes[@]}; do
	echo rack_size: $f
	conf=sensitivity_rack_size_$f.toml
	cp sensitivity_rack_size_base.toml $conf
	rack_bw=`python3 -c "print('{:.0f}'.format(${f} * 100 / ${oversub}))"`
	echo rack_bw: ${rack_bw}
	sed -i "s/^rack_size = 0/rack_size = $f/" $conf
	sed -i "s/^rack_bw = 0/rack_bw = ${rack_bw}/" $conf
	sed -i "s/sensitivity_rack_size_base/sensitivity_rack_size_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
