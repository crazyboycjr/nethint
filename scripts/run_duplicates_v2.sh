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
	sampler_port=`expr 6343 + $i`

	RUST_BACKTRACE=full \
	NH_CONTROLLER_URI=danyang-01.cs.duke.edu:9000 \
	NH_NUM_RACKS=$num_racks \
		target/release/nhagent_v2 \
		--shadow-id $i \
		-p $sampler_port \
		-i 100 \
		-b 10000000000:1:5:0.1 \
		arbitrary $num_racks 3 10 10 \
		&
		# --disable-v2 \
done

# DIR=$(dirname `realpath $0`)
# nix develop $DIR/../nethint-bpf -c \
# 	sudo -E NH_LOG=info RUST_BACKTRACE=1 \
# 	$DIR/../nethint-bpf/target/debug/nethint-user \
# 	arbitrary $num_racks 3 10 10

wait
