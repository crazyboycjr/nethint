Here's the plan

- Brain -> Cloud controller, VM allocator
- VirtCluster
- Sampler
- NetHint
-[o] Flow
-[o] Trace
-[o] Event
-[o] Simulator
-[o] Application
-[o] Topology trait
-[o] Cluster, physical topology
- BackgroundFlow
- MapReduce Example
- Allreduce Example



```
RUST_LOG=debug,nethint::cluster=info cargo run --release -- help
RUST_LOG=debug,nethint::cluster=info cargo run --release -- -a -m 20 -r 4 -s uniform_1000000 -n 10 virtual 150 4 100 100
```
