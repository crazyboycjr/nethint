use nethint::cluster::{LinkIx, Topology};
use nhagent::{
    self,
    cluster::{hostname, CLUSTER},
    communicator::Communicator,
    sampler::CounterUnit,
    Role,
};
use std::{borrow::Borrow, sync::mpsc};

use anyhow::Result;

use litemsg::endpoint;
use structopt::StructOpt;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

use nhagent::message;

static TERMINATE: AtomicBool = AtomicBool::new(false);

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

    log::info!("cluster: {}", CLUSTER.lock().unwrap().inner().to_dot());
    let my_role = CLUSTER.lock().unwrap().get_my_role();

    let mut comm = nhagent::communicator::Communicator::new(my_role)?;

    main_loop(rx, opt.interval_ms, &mut comm).unwrap();

    sampler.join().unwrap();
    Ok(())
}

fn main_loop(
    rx: mpsc::Receiver<Vec<CounterUnit>>,
    _interval_ms: u64,
    comm: &mut Communicator,
) -> anyhow::Result<()> {
    let mut handler = Handler::new();

    use message::Message;
    {
        let msg =
            Message::DeclareEthHostTable(CLUSTER.borrow().lock().unwrap().eth_hostname().clone());
        comm.broadcast(&msg)?;
    }
    {
        let msg = Message::DeclareHostname(hostname().clone());
        comm.broadcast(&msg)?;
    }
    comm.barrier(0);

    // let sleep = std::time::Duration::from_millis(interval_ms);
    // loop {
    //     for v in rx.try_iter() {
    //         for c in v {
    //             log::info!("counterunit: {:?}", c);
    //         }
    //     }
    //     // std::thread::sleep(sleep);
    // }

    log::info!("entering main loop");

    let poll = mio::Poll::new()?;

    let mut events = mio::Events::with_capacity(256);

    for i in 0..comm.world_size() {
        if i == comm.my_rank() {
            continue;
        }
        let ep = comm.peer(i);
        log::trace!("registering ep[{}].interest: {:?}", i, ep.interest());
        let interest = ep.interest();
        poll.register(ep.stream(), mio::Token(i), interest, mio::PollOpt::level())?;
    }

    let timeout = std::time::Duration::from_millis(1);

    while !TERMINATE.load(SeqCst) {
        for v in rx.try_iter() {
            for c in v {
                log::info!("counterunit: {:?}", c);
            }
        }

        poll.poll(&mut events, Some(timeout))?;

        for event in events.iter() {
            let rank = event.token().0;
            assert!(rank < comm.world_size());
            assert!(rank != comm.my_rank());

            let ep = comm.peer_mut(rank);

            if event.readiness().is_writable() {
                match ep.on_send_ready() {
                    Ok(attachment) => {
                        handler.on_send_complete(attachment)?;
                    }
                    Err(endpoint::Error::NothingToSend) => {}
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
            if event.readiness().is_readable() {
                match ep.on_recv_ready() {
                    Ok((cmd, _)) => {
                        std::mem::drop(ep);
                        handler.on_recv_complete(cmd, rank, comm)?;
                    }
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(endpoint::Error::ConnectionLost) => {
                        poll.deregister(ep.stream()).unwrap();
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    Ok(())
}

// service handler
struct Handler {
    traffic: HashMap<LinkIx, Vec<CounterUnit>>,
    rank_hostname: HashMap<usize, String>,
}

impl Handler {
    fn new() -> Self {
        Handler {
            traffic: Default::default(),
            rank_hostname: Default::default(),
        }
    }

    fn on_send_complete(&mut self, _attachment: Option<Vec<u8>>) -> anyhow::Result<()> {
        Ok(())
    }

    fn on_recv_complete(
        &mut self,
        msg: message::Message,
        sender_rank: usize,
        comm: &mut Communicator,
    ) -> anyhow::Result<()> {
        use message::Message::*;
        match msg {
            AppFinish => {
                TERMINATE.store(true, SeqCst);
            }
            DeclareEthHostTable(table) => {
                // merge table from other hosts
                let mut pcluster = CLUSTER.borrow().lock().unwrap();
                pcluster.update_eth_hostname(table);
            }
            DeclareHostname(hostname) => {
                self.rank_hostname.insert(sender_rank, hostname);
            }
            ServerChunk(chunk) => {
                // 1. parse chunk, aggregate, and send aggregated information to other racks
                let pcluster = CLUSTER.lock().unwrap();
                let rack_ix = pcluster.get_my_rack_ix();
                let rack_name = &pcluster.inner()[rack_ix].name;
                let rack_uplink = pcluster.inner().get_uplink(rack_ix);
                let dataunit = self.traffic.entry(rack_uplink).or_insert(vec![CounterUnit::new(rack_name)]);
                for c in chunk {
                    use nhagent::sampler::CounterType::*;
                    dataunit[0].data[Tx] += c.data[Tx] - c.data[TxIn];
                    dataunit[0].data[Rx] += c.data[Rx] - c.data[RxIn];
                }
                // 2. save chunk
                let my_role = pcluster.get_my_role();
                assert_eq!(my_role, Role::RackLeader);
                let sender_hostname = &self.rank_hostname[&sender_rank];
                let uplink = pcluster.inner().get_uplink(pcluster.inner().get_node_index(sender_hostname));
                *self.traffic.entry(uplink).or_insert_with(Vec::new) = chunk;
            }
            RackChunk(chunk) => {
                let pcluster = CLUSTER.lock().unwrap();
                let my_role = pcluster.get_my_role();
                assert_eq!(my_role, Role::RackLeader);
            }
            AllHints(allhints) => {
            }
            SyncRequest(_) | SyncResponse(_) => panic!("impossible"),
        }

        Ok(())
    }
}
