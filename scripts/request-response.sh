#!/bin/bash

WARMUP=10
ITERS=100
DATA_SIZE=224

ID=`hostname | cut -d '-' -f2`
if [ $ID -eq 1 ]; then
	cargo run --release --example request-response -- --server --num_clients 20 --data_size $DATA_SIZE --warmup $WARMUP --iters $ITERS
else
	for i in {1..4}; do
		cargo run --release --example request-response -- --client rdma0.danyang-01 --data_size $DATA_SIZE --warmup $WARMUP --iters $ITERS &
	done
	wait
fi
