#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

overlapped_jobs=(
5
10
20
30
)

rack_sizes=(
7
15
30
40
)

rack_bws=(
233
500
1000
1333
)

idx=0
for n in ${overlapped_jobs[@]}; do
	echo n: $n
	rack_size=${rack_sizes[$idx]}
	rack_bw=${rack_bws[$idx]}
	echo rack_size: $rack_size
	echo rack_bw: $rack_bw
	conf=allreduce_herd_$n.toml
	cp allreduce_herd_base.toml $conf
	sed -i "s/^ncases = 40/ncases = ${n}/" $conf
	sed -i "s/^rack_size = 40/rack_size = ${rack_size}/" $conf
	sed -i "s/^rack_bw = 1333/rack_bw = ${rack_bw}/" $conf
	sed -i "s/allreduce_herd_base/allreduce_herd_$n/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
	idx=`expr $idx + 1`
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
