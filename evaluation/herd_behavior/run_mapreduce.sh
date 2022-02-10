#!/bin/bash
# This last experiment can take 24 hours to run
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

overlapped_jobs=(
5
10
20
30
)

time_scales=(
1
0.5
0.2
0.1
)

idx=0
for f in ${overlapped_jobs[@]}; do
	echo overlapped_jobs: $f
	conf=mapreduce_herd_$f.toml
	cp mapreduce_herd_base.toml $conf
	time_scale=${time_scales[$idx]}
	echo time_scale: $time_scale
	sed -i "s/^time_scale = 1/time_scale = ${time_scale}/" $conf
	sed -i "s/mapreduce_herd_base/mapreduce_herd_$f/" $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -P 5 -c $conf &
	idx=`expr $idx + 1`
	cnt=`expr $cnt + 5` # 5 threads
	[[ $cnt -ge $(nproc) ]] && { wait; cnt=`expr $cnt - 5`; }
done

wait
