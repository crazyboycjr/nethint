#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

for conf in `ls *.toml`; do
	echo $conf
	RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c $conf &
done

# {
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment1.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment2.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment3.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment4.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment5.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment6.toml
# } &
# 
# {
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment7.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment8.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment9.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment10.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment11.toml
# RUST_BACKTRACE=1 RUST_LOG=error cargo run --bin mapreduce_experiment --release -- -c experiment12.toml
# } &

wait
