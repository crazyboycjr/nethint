use nethint::architecture::TopoArgs;
use nethint::background_flow_hard::BackgroundFlowHard;
use structopt::StructOpt;
use std::net::SocketAddr;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "nhagent", about = "NetHint Agent")]
pub struct Opts {
    /// The working interval of agent in millisecond
    #[structopt(short = "i", long = "interval", default_value = "100")]
    pub interval_ms: u64,

    /// The listening port of the sampler
    #[structopt(short = "p", long, default_value = "6343")]
    pub sampler_listen_port: u16,

    /// Specify the topology for testbed
    #[structopt(subcommand)]
    pub topo: TopoArgs,

    /// Background flow parameter by enforcing rate limit, the
    /// format is freq:prob:amp[:avg_load]
    #[structopt(short, long, default_value)]
    pub background_flow_hard: BackgroundFlowHard,

    /// When specified, it represents the number of the duplicated agent.
    /// This option is only used to measure the system overhead by running
    /// multiple nhagents on the same servers.
    #[structopt(short, long)]
    pub shadow_id: Option<usize>,

    /// Disable HetHint v2, and only run NetHint v1.
    #[structopt(short, long)]
    pub disable_v2: bool,

    // the two fields below are used by the BPF userspace program
    /// Physical interface name for the BPF program
    #[structopt(long)]
    pub iface: Option<String>,

    /// Rack leader address (ip:port)
    #[structopt(long)]
    pub rack_leader: Option<SocketAddr>,
}
