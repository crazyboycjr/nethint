#!/bin/bash

cargo build --bin nhagent_v2

cargo build --release --bin controller
cargo build --release --bin worker
cargo build --release --bin rplaunch
cargo build --release --bin nhagent_v2
cargo build --release --bin scheduler
cargo build --release --bin ssagent
