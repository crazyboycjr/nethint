#!/bin/bash

cargo build --bin nhagent

cargo build --release --bin controller
cargo build --release --bin worker
cargo build --release --bin rplaunch
cargo build --release --bin nhagent
cargo build --release --bin scheduler
