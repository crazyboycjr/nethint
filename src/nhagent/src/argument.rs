use nethint::architecture::TopoArgs;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "nhagent", about = "NetHint Agent")]
pub struct Opts {
    /// The working interval of agent in millisecond
    #[structopt(short = "i", long = "interval", default_value = "100")]
    pub interval_ms: u64,
    /// The listening port of the sampler
    #[structopt(short = "p", long = "p", default_value = "5555")]
    pub sampler_listen_port: u16,
    /// Specify the topology for testbed
    #[structopt(subcommand)]
    pub topo: TopoArgs,
}