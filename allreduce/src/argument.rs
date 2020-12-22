use nethint::architecture::TopoArgs;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "AllReduce", about = "AllReduce Application")]
pub struct Opt {
    /// Specify the topology for simulation
    #[structopt(subcommand)]
    pub topo: TopoArgs,
}