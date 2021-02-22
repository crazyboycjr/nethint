use std::sync::mpsc;
use nhagent::{self, sampler::CounterUnit};
use nethint::cluster::Topology;

use anyhow::Result;

fn main() -> Result<()> {
    logging::init_log();
    log::info!("Starting nhagent...");

    let (tx, rx) = mpsc::channel();

    let interval_ms = 100;
    let mut sampler = nhagent::sampler::OvsSampler::new(interval_ms, tx);
    sampler.run();

    let cluster = nhagent::cluster::init_cluster();
    log::info!("cluster: {}", cluster.to_dot());
    let my_role = nhagent::cluster::get_my_role(&cluster);

    // let mut comm = nhagent::communicator::Communicator::new(my_role)?;

    main_loop(rx, interval_ms).unwrap();

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
    Ok(())
}
