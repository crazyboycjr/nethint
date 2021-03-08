#![feature(option_unwrap_none)]
#![feature(map_into_keys_values)]

use nethint::cluster::{LinkIx, SLinkIx, Topology};
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
    interval_ms: u64,
    comm: &mut Communicator,
) -> anyhow::Result<()> {
    log::info!("entering main loop");

    let mut handler = Handler::new(comm);

    use message::Message;
    {
        let msg =
            Message::DeclareEthHostTable(CLUSTER.borrow().lock().unwrap().eth_hostname().clone());
        log::debug!("broadcasting: {:?}", msg);
        comm.broadcast(&msg)?;
    }
    {
        let msg = Message::DeclareHostname(hostname().clone());
        log::debug!("broadcasting: {:?}", msg);
        comm.broadcast(&msg)?;
    }
    comm.barrier(0)?;
    log::debug!("barrier 0 passed");

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

    let interval = std::time::Duration::from_millis(interval_ms);
    let mut last_tp = std::time::Instant::now();

    // to ensure poll call won't block forever
    let timeout = std::time::Duration::from_millis(1);

    while !TERMINATE.load(SeqCst) {
        for v in rx.try_iter() {
            for c in &v {
                log::info!("counterunit: {:?}", c);
            }
            // receive from local sampler module
            handler.receive_server_chunk(comm.my_rank(), v);
        }

        let now = std::time::Instant::now();
        if now >= last_tp + interval {
            // it's not very explicit why this is is correct or not, need more thinking on this
            if comm.my_role() == Role::RackLeader || comm.my_role() == Role::GlobalLeader {
                handler.send_allhints(comm)?;
                handler.send_rack_chunk(comm)?;
            } else {
                handler.send_server_chunk(comm)?;
            }
            handler.reset_traffic();
            last_tp = now;
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
    fn new(comm: &mut Communicator) -> Self {
        Handler {
            traffic: Default::default(),
            rank_hostname: std::iter::once((comm.my_rank(), nhagent::cluster::hostname().clone()))
                .collect(),
        }
    }

    fn reset_traffic(&mut self) {
        self.traffic = Default::default();
    }

    fn send_server_chunk(&mut self, comm: &mut Communicator) -> anyhow::Result<()> {
        let pcluster = CLUSTER.lock().unwrap();
        let my_rank = comm.my_rank();
        let my_node_ix = pcluster.my_node_ix();
        let uplink = pcluster.inner().get_uplink(my_node_ix);
        if self.traffic.contains_key(&uplink) {
            // find my rack leader, and send msg to it
            let mut my_rack_leader_rank = None;
            for (&i, h) in &self.rank_hostname {
                let node_ix = pcluster.inner().get_node_index(h);
                if pcluster.get_role(&*h) == Role::RackLeader
                    && pcluster.is_same_rack(node_ix, my_node_ix)
                {
                    // find the rack leader
                    my_rack_leader_rank = Some(i);
                    break;
                }
            }
            // if not found, skip this sending
            if my_rack_leader_rank.is_some() {
                assert!(
                    my_rack_leader_rank.is_some(),
                    "my rack leader not found, my_rank: {}",
                    my_rank
                );
                let msg = message::Message::ServerChunk(self.traffic[&uplink].clone());
                comm.send_to(my_rack_leader_rank.unwrap(), &msg)?;
            }
        }
        Ok(())
    }

    fn receive_server_chunk(&mut self, sender_rank: usize, chunk: Vec<CounterUnit>) {
        // 1. parse chunk, aggregate, and send aggregated information to other racks
        let pcluster = CLUSTER.lock().unwrap();
        let rack_ix = pcluster.get_my_rack_ix();
        let rack_name = &pcluster.inner()[rack_ix].name;
        let rack_uplink = pcluster.inner().get_uplink(rack_ix);
        let dataunit = self
            .traffic
            .entry(rack_uplink)
            .or_insert(vec![CounterUnit::new(rack_name)]);
        for c in &chunk {
            use nhagent::sampler::CounterType::*;
            dataunit[0].data[Tx] += c.data[Tx] - c.data[TxIn];
            dataunit[0].data[Rx] += c.data[Rx] - c.data[RxIn];
        }
        // 2. save chunk
        let sender_hostname = &self.rank_hostname[&sender_rank];
        let uplink = pcluster
            .inner()
            .get_uplink(pcluster.inner().get_node_index(sender_hostname));
        // insert or merge
        if self.traffic.contains_key(&uplink) {
            *self.traffic.get_mut(&uplink).unwrap() =
                Self::merge_chunk(&self.traffic[&uplink], &chunk);
        } else {
            self.traffic.insert(uplink, chunk);
        }
    }

    fn merge_chunk(chunk1: &Vec<CounterUnit>, chunk2: &Vec<CounterUnit>) -> Vec<CounterUnit> {
        use std::collections::BTreeMap;
        let mut ret: BTreeMap<String, CounterUnit> = BTreeMap::new();
        for c in chunk1.iter().chain(chunk2.iter()) {
            if ret.contains_key(&c.vnodename) {
                ret.get_mut(&c.vnodename).unwrap().merge(c);
            } else {
                ret.insert(c.vnodename.clone(), c.clone());
            }
        }
        ret.into_values().collect()
    }

    // Assume SLinkIx from different agents are compatible with each other
    fn receive_rack_chunk(&mut self, chunk: HashMap<SLinkIx, Vec<CounterUnit>>) {
        // 1. parse chunk, aggregate
        for (l, c) in chunk {
            let l: LinkIx = l.into();
            if self.traffic.contains_key(&l) {
                *self.traffic.get_mut(&l).unwrap() = Self::merge_chunk(&self.traffic[&l], &c);
            } else {
                self.traffic.insert(l, c);
            }
        }
    }

    fn send_rack_chunk(&self, comm: &mut Communicator) -> anyhow::Result<()> {
        assert!(comm.my_role() == Role::RackLeader || comm.my_role() == Role::GlobalLeader);
        let pcluster = CLUSTER.lock().unwrap();
        // 1. get my rack chunk
        let mut my_rack_traffic: HashMap<LinkIx, Vec<CounterUnit>> = Default::default(); // it's just a subtree of self.traffic
        let my_node_ix = pcluster.my_node_ix();
        let my_rack_ix = pcluster
            .inner()
            .get_target(pcluster.inner().get_uplink(my_node_ix));
        for l in pcluster.inner().get_downlinks(my_rack_ix) {
            let l = pcluster.inner().get_reverse_link(*l);
            my_rack_traffic
                .insert(l, self.traffic.get(&l).cloned().unwrap_or_default())
                .unwrap_none();
        }
        let my_rack_uplink = pcluster.inner().get_uplink(my_rack_ix);
        my_rack_traffic
            .insert(
                my_rack_uplink,
                self.traffic
                    .get(&my_rack_uplink)
                    .cloned()
                    .unwrap_or_default(),
            )
            .unwrap_none();
        // 2. send to all other rack leaders
        let my_rack_traffic_ser = my_rack_traffic
            .into_iter()
            .map(|(k, v)| (SLinkIx(k), v))
            .collect();
        let msg = message::Message::RackChunk(my_rack_traffic_ser);
        for i in 0..comm.world_size() {
            if i == comm.my_rank() {
                continue;
            }
            let peer_hostname = &self.rank_hostname[&i];
            let peer_ix = pcluster.inner().get_node_index(peer_hostname);
            if pcluster.get_role(peer_hostname) != Role::Worker
                && pcluster.is_cross_rack(my_node_ix, peer_ix)
            {
                comm.send_to(i, &msg)?;
            }
        }
        Ok(())
    }

    fn send_allhints(&self, comm: &mut Communicator) -> anyhow::Result<()> {
        assert!(comm.my_role() == Role::RackLeader || comm.my_role() == Role::GlobalLeader);
        let pcluster = CLUSTER.lock().unwrap();
        let chunk = self
            .traffic
            .iter()
            .map(|(&k, v)| (SLinkIx(k), v.clone()))
            .collect();
        let msg = message::Message::AllHints(chunk);
        // send to all workers within the same rack
        let my_node_ix = pcluster.my_node_ix();
        for i in 0..comm.world_size() {
            if i == comm.my_rank() {
                continue;
            }
            let peer_hostname = &self.rank_hostname[&i];
            let peer_ix = pcluster.inner().get_node_index(peer_hostname);
            if pcluster.get_role(peer_hostname) == Role::Worker
                && pcluster.is_same_rack(my_node_ix, peer_ix)
            {
                comm.send_to(i, &msg)?;
            }
        }
        Ok(())
    }

    fn receive_allhints(
        &mut self,
        allhints: HashMap<SLinkIx, Vec<CounterUnit>>,
    ) -> anyhow::Result<()> {
        let pcluster = CLUSTER.lock().unwrap();
        let my_role = pcluster.get_my_role();
        assert_eq!(my_role, Role::Worker);
        self.receive_rack_chunk(allhints);
        log::info!("worker agent link traffic: {:?}", self.traffic);
        Ok(())
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
                let my_role = comm.my_role();
                assert_eq!(my_role, Role::RackLeader);
                self.receive_server_chunk(sender_rank, chunk);
            }
            RackChunk(chunk) => {
                let my_role = comm.my_role();
                assert_ne!(my_role, Role::Worker);
                self.receive_rack_chunk(chunk);
                log::info!("rack leader agent link traffic: {:?}", self.traffic);
            }
            AllHints(allhints) => {
                self.receive_allhints(allhints)?;
            }
            SyncRequest(_) | SyncResponse(_) => panic!("impossible"),
        }

        Ok(())
    }
}
