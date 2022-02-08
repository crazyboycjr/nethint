#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

oversub=3
rack_sizes=(
5
10
20
)

cnt=0
for f in ${rack_sizes[@]}; do
	echo rack_size: $f
	conf=sensitivity_rack_size_$f.toml
	cp sensitivity_rack_size_base.toml $conf
	rack_bw=`python3 -c "print('{:.0f}'.format(${f} * 100 / ${oversub}))"`
	job_size_distribution=`python3 -c "print('[[80, {}], [80, {}]]'.format($f // 5 * 8, $f // 5 * 12))"`
	echo rack_bw: ${rack_bw}
	echo job_size_distribution: ${job_size_distribution}
	sed -i "s/job_size_distribution = [[80, 8], [80, 12]]/job_size_distribution = ${job_size_distribution}/" $conf
	sed -i "s/^rack_size = 0/rack_size = $f/" $conf
	sed -i "s/^rack_bw = 0/rack_bw = ${rack_bw}/" $conf
	sed -i "s/sensitivity_rack_size_base/sensitivity_rack_size_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
