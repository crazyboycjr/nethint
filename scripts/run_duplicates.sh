#!/bin/bash

if [ $UID -ne 0 ]; then
	echo "Please run $0 as root"
	exit 3
fi

if [ $# -ne 1 ]; then
	echo "Usage: $0 <scale>"
	exit 1
fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

scale=$1
num_workers=`expr 6 \* $scale`
num_racks=`expr 2 \* $scale`

for ((i=0; i<$scale; i++)); do
	sampler_port=`expr 5555 + $i`

	RUST_BACKTRACE=full \
	NH_CONTROLLER_URI=192.168.211.2:9000 \
	NH_NUM_WORKER=$num_workers \
		target/release/nhagent \
		--shadow-id $i \
		-p $sampler_port \
		-i 100 \
		-b 800000000000000:1:5:0.1 \
		arbitrary $num_racks 3 10 10 \
		&
		# --disable-v2 \
done

wait
