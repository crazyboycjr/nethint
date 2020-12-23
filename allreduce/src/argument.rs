use nethint::architecture::TopoArgs;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "AllReduce", about = "AllReduce Application")]
pub struct Opt {
    /// Specify the topology for simulation
    #[structopt(subcommand)]
    pub topo: TopoArgs,

    /// Number of reduce tasks. When using trace, this parameter means reduce scale factor
    #[structopt(short = "w", long = "num_workers", default_value = "16")]
    pub num_workers: usize,
    
    /// Number of testcases
    #[structopt(short = "n", long = "ncases", default_value = "1")]
    pub ncases: usize,
}