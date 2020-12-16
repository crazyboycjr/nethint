use structopt::StructOpt;
use nethint::architecture::TopoArgs;

use crate::{JobSpec, ShuffleDist};

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "MapReduce", about = "MapReduce Application")]
pub struct Opt {
    /// Specify the topology for simulation
    #[structopt(subcommand)]
    pub topo: TopoArgs,

    /// Asymmetric bandwidth
    #[structopt(short = "a", long = "asymmetric")]
    pub asym: bool,

    /// Probability distribution of shuffle flows, examples: uniform_1000000, zipf_1000000_0.5
    #[structopt(
        short = "s",
        long = "shuffle",
        name = "distribution",
        default_value = "uniform_1000000"
    )]
    pub shuffle: ShuffleDist,

    /// Number of map tasks. When using trace, this parameter means map scale factor
    #[structopt(short = "m", long = "map", default_value = "4")]
    pub num_map: usize,

    /// Number of reduce tasks. When using trace, this parameter means reduce scale factor
    #[structopt(short = "r", long = "reduce", default_value = "4")]
    pub num_reduce: usize,

    /// Number of testcases
    #[structopt(short = "n", long = "ncases", default_value = "10")]
    pub ncases: usize,

    /// Run experiments from trace file
    #[structopt(short = "f", long = "file")]
    pub trace: Option<std::path::PathBuf>,

    /// Output path of the figure
    #[structopt(short = "d", long = "directory")]
    pub directory: Option<std::path::PathBuf>,

    /// Run simulation experiments in parallel, default using the hardware concurrency
    #[structopt(short = "P", long = "parallel", name = "nthreads")]
    pub parallel: Option<usize>,

    /// Normalize, draw speed up instead of absolution job completion time
    #[structopt(short = "N", long = "normalize")]
    pub normalize: bool,

    /// Inspect the trace file, see the overlap among multiple jobs
    #[structopt(long = "inspect")]
    pub inspect: bool,
}

// impl Opt {
//     fn to_title(&self, prefix: &str) -> String {
//         format!("MapReduce CDF ")
//     }
// }

impl Opt {
    pub fn to_filename(&self, prefix: &str) -> String {
        if let Some(_f) = self.trace.as_ref() {
            format!(
                "{}_{}_from_trace_m{}_r{}.pdf",
                prefix, self.topo, self.num_map, self.num_reduce
            )
        } else {
            let job_spec = JobSpec::new(self.num_map, self.num_reduce, self.shuffle.clone());
            format!("{}_{}_{}.pdf", prefix, self.topo, job_spec)
        }
    }
}