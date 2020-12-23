use nethint::architecture::TopoArgs;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "AllReduce", about = "AllReduce Application")]
pub struct Opt {
    /// Specify the topology for simulation
    #[structopt(subcommand)]
    pub topo: TopoArgs,

    /// Number of workers.
    #[structopt(short = "w", long = "num_workers", default_value = "16")]
    pub num_workers: usize,

    /// Buffer size of allreduce.
    #[structopt(short = "s", long = "buffer_size", default_value = "1000000")]
    pub buffer_size: usize,

    /// Number of allreduce iterations.
    #[structopt(short = "i", long = "num_iterations", default_value = "1000000")]
    pub num_iterations: usize,

    /// Number of jobs.
    #[structopt(short = "n", long = "ncases", default_value = "1")]
    pub ncases: usize,
}