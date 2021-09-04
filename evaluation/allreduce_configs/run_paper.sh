#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

# enable computation time
# sed -i '/^buffer_size/a computation_speed = 0.1' *.toml

# use per tenant fairness
# sed -i 's/^fairness = "PerFlowMaxMin"/fairness = "TenantFlowMaxMin"/' *.toml

configs=(
# standard2.toml
standard3.toml
# casestudy2.toml
# nonnegligible_computing_overhead.toml
standard3_pervm.toml
standard3_pertenant.toml
# background_dynamic_strong.toml
# background_off.toml
# background_static_strong.toml
# level2bad.toml
)

for conf in ${configs[@]}; do
	echo $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin allreduce_experiment --release -- -P 5 -c $conf &
done

wait
