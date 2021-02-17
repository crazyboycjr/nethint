use std::sync::mpsc;
use nhagent;

use anyhow::Result;

fn main() -> Result<()> {
    logging::init_log();
    log::info!("Starting nhagent...");

    let (tx, rx) = mpsc::channel();

    let interval_ms = 100;
    let mut sampler = nhagent::sampler::OvsSampler::new(interval_ms, tx);
    sampler.run();

    let cluster = nhagent::cluster::init_cluster();
    let my_role = nhagent::cluster::get_my_role(&cluster);

    let mut comm = nhagent::communicator::Communicator::new(my_role)?;

    main_loop(rx, interval_ms).unwrap();

    sampler.join().unwrap();
    Ok(())
}

fn main_loop() -> anyhow::Result<()> {
    Ok(())
}
