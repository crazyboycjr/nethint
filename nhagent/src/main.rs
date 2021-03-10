#![feature(option_unwrap_none)]
#![feature(map_into_keys_values)]

use nethint::{
    brain::{BrainSetting, PlacementStrategy},
    cluster::{LinkIx, Topology, VirtCluster},
    counterunit::CounterUnit,
    hint::{NetHintV1Real, NetHintV2Real, NetHintVersion},
};
use nhagent::{
    self,
    cluster::{hostname, CLUSTER},
    communicator::Communicator,
    Role,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc;

use anyhow::Result;

use litemsg::endpoint;
use structopt::StructOpt;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

use nhagent::message;

use nethint::brain::Brain;

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

    let mut handler = Handler::new(comm, interval_ms);

    use message::Message;
    {
        let msg = Message::DeclareEthHostTable(CLUSTER.lock().unwrap().eth_hostname().clone());
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

    // if this is a global leader, also add the controller listener to serve cloud provision request
    if comm.my_role() == Role::GlobalLeader {
        let listener = comm.controller_listener().unwrap();
        poll.register(
            listener,
            mio::Token(comm.world_size()),
            mio::Ready::readable(),
            mio::PollOpt::level(),
        )?;
    }

    let interval = std::time::Duration::from_millis(interval_ms);
    let mut last_tp = std::time::Instant::now();

    // to ensure poll call won't block forever
    let timeout = std::time::Duration::from_millis(1);

    while !TERMINATE.load(SeqCst) {

        let now = std::time::Instant::now();
        if now >= last_tp + interval {
            // it's not very explicit why this is is correct or not, need more thinking on this
            handler.reset_traffic();

            for v in rx.try_iter() {
                for c in &v {
                    log::info!("counterunit: {:?}", c);
                }
                // receive from local sampler module
                handler.receive_server_chunk(comm.my_rank(), v);
            }

            if comm.my_role() == Role::RackLeader || comm.my_role() == Role::GlobalLeader {
                handler.send_allhints(comm)?;
                handler.send_rack_chunk(comm)?;
            } else {
                handler.send_server_chunk(comm)?;
            }

            last_tp = now;
        }

        poll.poll(&mut events, Some(timeout))?;

        for event in events.iter() {
            let rank = event.token().0;
            // log::debug!("handle event from rank: {}", rank);

            if rank == comm.world_size() {
                // it must be the controller listener wants to accept new connections
                assert!(event.readiness().is_readable());
                comm.accept_app_connection(&poll)?;
                continue;
            }

            assert!(rank != comm.my_rank());

            let ep = if rank > comm.world_size() {
                // it must be the controller, and the request comes from the apps
                comm.app_ep_mut(rank)
            } else {
                comm.peer_mut(rank)
            };

            if event.readiness().is_writable() {
                // log::debug!("handle write event for rank: {}", rank);
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
                // klog::debug!("handle read event for rank: {}", rank);
                match ep.on_recv_ready() {
                    Ok((cmd, _)) => {
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
    brain: Rc<RefCell<Brain>>,
    interval_ms: u64,
}

impl Handler {
    fn new(comm: &mut Communicator, interval_ms: u64) -> Self {
        let brain_setting = BrainSetting {
            seed: 1,
            asymmetric: false,
            broken: 0.0,
            max_slots: nhagent::cluster::MAX_SLOTS,
            sharing_mode: nethint::SharingMode::Guaranteed,
            guaranteed_bandwidth: Some(25),
            topology: nethint::architecture::TopoArgs::Arbitrary {
                nracks: nhagent::cluster::NRACKS,
                rack_size: nhagent::cluster::RACK_SIZE,
                host_bw: nhagent::cluster::HOST_BW,
                rack_bw: nhagent::cluster::RACK_BW,
            },
            background_flow_high_freq: Default::default(),
        };
        let brain = Brain::build_cloud(brain_setting);
        Handler {
            traffic: HashMap::<LinkIx, Vec<CounterUnit>>::new(),
            rank_hostname: std::iter::once((comm.my_rank(), nhagent::cluster::hostname().clone()))
                .collect(),
            brain,
            interval_ms,
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
            use nethint::counterunit::CounterType::*;
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

    // Assume LinkIx from different agents are compatible with each other
    fn receive_rack_chunk(&mut self, chunk: HashMap<LinkIx, Vec<CounterUnit>>) {
        // 1. parse chunk, aggregate
        for (l, c) in chunk {
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
        let msg = message::Message::RackChunk(my_rack_traffic);
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
        let chunk = self.traffic.iter().map(|(&k, v)| (k, v.clone())).collect();
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
        allhints: HashMap<LinkIx, Vec<CounterUnit>>,
    ) -> anyhow::Result<()> {
        let pcluster = CLUSTER.lock().unwrap();
        let my_role = pcluster.get_my_role();
        assert_eq!(my_role, Role::Worker);
        self.receive_rack_chunk(allhints);
        log::info!("worker agent link traffic: {:?}", self.traffic);
        Ok(())
    }

    fn get_vname_to_hostname(vc: &VirtCluster) -> HashMap<String, String> {
        (0..vc.num_hosts())
            .map(|vid| {
                // 0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 8, 5 -> 9
                let vname = format!("host_{}", vid);
                let pname = vc.virt_to_phys()[&vname].clone();
                let pid: usize = pname.strip_prefix("host_").unwrap().parse().unwrap();
                let hostid = (pid / 4) * 8 + pid % 4;
                (vname, format!("cpu{}", hostid))
            })
            .collect()
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
        log::trace!("on_recv_complete, sender_rank: {} msg: {:?}", sender_rank, msg);
        match msg {
            AppFinish => {
                TERMINATE.store(true, SeqCst);
            }
            DeclareEthHostTable(table) => {
                // merge table from other hosts
                let mut pcluster = CLUSTER.lock().unwrap();
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
            // serve as cloud brain, by global leader
            ProvisionRequest(tenant_id, nhosts_to_acquire) => {
                // connect the code to Brain, provision VMs, return the vcluster
                let vc = self
                    .brain
                    .borrow_mut()
                    .provision(tenant_id, nhosts_to_acquire, PlacementStrategy::Compact)
                    .unwrap();
                // in our testbed, the virtual machine hostname are just cpu{}
                // so there is a direct translation between vnode to hostname
                let vname_to_hostname = Self::get_vname_to_hostname(&vc);
                let hintv1 = NetHintV1Real {
                    vc,
                    vname_to_hostname,
                };
                let msg = ProvisionResponse(tenant_id, hintv1);
                comm.send_to(sender_rank, &msg)?;
            }
            DestroyRequest(tenant_id) => {
                // destroy and release the resource immediately in testbed
                self.brain.borrow_mut().destroy(tenant_id);
                self.brain.borrow_mut().garbage_collect(tenant_id + 1);
                let msg = DestroyResponse(tenant_id);
                comm.send_to(sender_rank, &msg)?;
            }
            NetHintRequest(tenant_id, version) => {
                match version {
                    NetHintVersion::V1 => {
                        let vc =
                            (*self.brain.borrow().vclusters()[&tenant_id].borrow()).clone();
                        let vname_to_hostname = Self::get_vname_to_hostname(&vc);
                        let hintv1 = NetHintV1Real {
                            vc,
                            vname_to_hostname,
                        };
                        let msg = NetHintResponseV1(tenant_id, hintv1);
                        comm.send_to(sender_rank, &msg)?;
                    }
                    NetHintVersion::V2 => {
                        // just extract the traffic information from physical cluster: for each virtual link, find the physical link, and grab the traffic from it
                        let mut traffic: HashMap<LinkIx, Vec<CounterUnit>> = Default::default();
                        let mut vc =
                            (*self.brain.borrow().vclusters()[&tenant_id].borrow()).clone();
                        for vlink_ix in vc.all_links() {
                            let phys_link = nethint::hint::get_phys_link(
                                &*self.brain.borrow(),
                                tenant_id,
                                vlink_ix,
                            );
                            let traffic_on_link = self.traffic[&phys_link].clone();
                            // TODO(cjr): we need to do some modifications to traffic_on_link
                            // because it contains all traffic from all tenants
                            // the traffic from the requestor itself must be subtracted
                            traffic.insert(vlink_ix, traffic_on_link);
                            // set vc.bandwidth to the value of physical links
                            vc[vlink_ix].bandwidth =
                                self.brain.borrow().cluster()[phys_link].bandwidth;
                        }
                        let vname_to_hostname = Self::get_vname_to_hostname(&vc);
                        let hintv1 = NetHintV1Real {
                            vc,
                            vname_to_hostname,
                        };
                        let hintv2 = NetHintV2Real { hintv1, interval_ms: self.interval_ms, traffic };
                        let msg = NetHintResponseV2(tenant_id, hintv2);
                        comm.send_to(sender_rank, &msg)?;
                    }
                }
            }
            SyncRequest(_)
            | SyncResponse(_)
            | ProvisionResponse(..)
            | NetHintResponseV1(..)
            | NetHintResponseV2(..)
            | DestroyResponse(_) => {
                panic!("shouldn't receive this msg: {:?}", msg);
            }
        }

        Ok(())
    }
}
