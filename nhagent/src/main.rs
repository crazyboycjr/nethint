use std::sync::mpsc;
use nhagent::{self, sampler::CounterUnit};
use nethint::cluster::Topology;

use anyhow::Result;

use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "nhagent", about = "NetHint Agent")]
struct Opts {
    /// The working interval of agent in millisecond
    #[structopt(short = "i", long = "interval", default_value = "100")]
    interval_ms: u64,
}

fn main() -> Result<()> {
    logging::init_log();
    log::info!("Starting nhagent...");

    let opt = Opts::from_args();
    log::info!("Opts: {:#?}", opt);

    // TODO(cjr): put these code in NetHintAgent struct
    let (tx, rx) = mpsc::channel();

    let mut sampler = nhagent::sampler::OvsSampler::new(opt.interval_ms, tx);
    sampler.run();

    let cluster = nhagent::cluster::init_cluster();
    log::info!("cluster: {}", cluster.to_dot());
    let my_role = nhagent::cluster::get_my_role(&cluster);

    // let mut comm = nhagent::communicator::Communicator::new(my_role)?;

    main_loop(rx, opt.interval_ms).unwrap();

    sampler.join().unwrap();
    Ok(())
}

fn main_loop(rx: mpsc::Receiver<Vec<CounterUnit>>, interval_ms: u64) -> anyhow::Result<()> {
    let sleep = std::time::Duration::from_millis(interval_ms);
    loop {
        for v in rx.try_iter() {
            for c in v {
                log::info!("counterunit: {:?}", c);
            }
        }
        std::thread::sleep(sleep);
    }
    unreachable!();
}
