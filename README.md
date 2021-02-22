# NetHint

```toml
# Specifiation of a MapReduce application experiment

# Run the experiments from trace file
trace = "mapreduce/FB2010-1Hr-150-0.txt"

# Number of testcases to run
ncases = 100

# Number of map tasks and reduce tasks
# When running from trace, these parameters become scale factors
num_map = 1
num_reduce = 1

# Multiply the traffic size by a number
traffic_scale = 100.0

# Mapper placement policy
mapper_policy = { type = "RandomSkew", args = [1, 0.2] }
# mapper_policy = { type = "Random", args = 1 }
# mapper_policy = { type = "Greedy", args = 1 }

placement_strategy = { type = "Compact" }
# placement_strategy = { type = "Spread" }
# placement_strategy = { type = "Random", args = 0 }

# Output path of for the simulation results
directory = "/tmp/mapr_result"

# Whether to allow a mapper to collocate with a reduce
collocate = true

# shuffle = { type = "FromTrace" }
# shuffle = { type = "Uniform", args = 1000000 }
# shuffle = { type = "Zipf", args = [ 1000000, 0.1 ] }

[[batch]]
reducer_policy = "Random"
probe = { enable = false }
# NetHint level, possible values are 0, 1, 2
nethint_level = 1

[[batch]]
reducer_policy = "HierarchicalGreedy"
probe = { enable = true, round_ms = 10 }
nethint_level = 1

[[batch]]
reducer_policy = "HierarchicalGreedy"
probe = { enable = false }
nethint_level = 1

[[batch]]
reducer_policy = "HierarchicalGreedy"
probe = { enable = false }
nethint_level = 2

[simulator]
nethint = true
sample_interval_ns = 100_000_000 # 100ms
loopback_speed = 400
fairness = "TenantFlowMinMax"
# fairness = "PerFlowMinMax"

# emulate background flow by subtracting a bandwidth to each link
# note that the remaining bandwidth must not smaller than link_bw / current_tenants
background_flow_hard = { enable = true, frequency_ns = 1_000_000_000, probability = 0.1 }
# background_flow_hard = { enable = false }
# nethint_delay_ms = 100

[brain]
# Random seed for multiple uses
seed = 1
# Whether the cluster's bandwidth is asymmetric
asymmetric = false
# The percentage of nodes marked broken
broken = 0.1
# The slots of each physical machine
max_slots = 4

# The topology for simulation
[brain.topology]
type = "Arbitrary"  # another possible value is "FatTree"

# [brain.topology.args] # When type = "FatTree"
# nports = 20           # the number of ports of a switch
# bandwidth = 100       # in Gbps
# oversub_ratio = 4.0   # oversubscription ratio

[brain.topology.args]         # When type = "Arbitrary"
nracks = 150        # the number of racks
rack_size = 20      # the number of hosts under a rack
host_bw = 100       # bandwidth of a host, in Gbps
rack_bw = 200       # bandwidth of a ToR switch, in Gbps

# [envs]
# NETHINT_PROVISION_LOG_FILE = "/tmp/f"
```

```
RUST_LOG=debug,nethint::cluster=info cargo run --bin mapreduce_experiment --release -- -c mapreduce/experiment.toml
```
