Here's the plan

- Flow
- Trace
- VTopo
- Event
- Simulator
- Application
- NetHint
- Sampler
- BackgroundFlow
- MapReduce Example
- Allreduce Example

dot visualization topology



```
RUST_LOG=debug,nethint::cluster=info cargo run --release -- help
RUST_LOG=debug,nethint::cluster=info cargo run --release -- -a -m 20 -r 4 -s uniform_1000000 -n 10 virtual 150 4 100 100
```
