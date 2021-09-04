#!/bin/bash

cargo build --bin nhagent_v2

cargo build --release --bin controller
cargo build --release --bin worker
cargo build --release --bin rplaunch
cargo build --release --bin nhagent_v2
cargo build --release --bin scheduler
# cargo build --release --bin ssagent

DIR=`cargo metadata --format-version 1 | jq -r '.workspace_root'`
builtin cd $DIR/../nethint-bpf

nix develop -c cargo build
nix develop -c cargo build --release

# build BPF program and its userspace program
# DIR=$(dirname `realpath $0`)
# nix develop $DIR/../nethint-bpf -c cargo build
# nix develop $DIR/../nethint-bpf -c cargo build --release
# for sec in {.BTF,.eh_frame,.text,.BTF.ext}; do
# 	nix develop $DIR/../nethint-bpf -c \
# 		llvm-strip --strip-unneeded --remove-section ${sec} \
# 		/nfs/cjr/Developing/nethint-bpf/target/debug/build/nethint-userspace-0abfae651d38dbe6/out/target/bpf/programs/nethint/nethint.elf
# done
