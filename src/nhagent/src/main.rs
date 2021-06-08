use nethint::{
    background_flow_hard::BackgroundFlowHard,
    brain::{BrainSetting, PlacementStrategy},
    cluster::{LinkIx, Topology, VirtCluster},
    counterunit::CounterUnit,
    hint::{NetHintV1Real, NetHintV2Real, NetHintVersion},
    TenantId,
};
use nhagent::{
    self,
    argument::Opts,
    cluster::{self, hostname, CLUSTER},
    communicator::Communicator,
    message,
    timing::{self, TimeList},
    Role,
};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::process::Command;
use std::rc::Rc;
use std::sync::mpsc;

use anyhow::Result;
use structopt::StructOpt;

use litemsg::endpoint;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

use nethint::brain::Brain;

static TERMINATE: AtomicBool = AtomicBool::new(false);

fn main() -> Result<()> {
    logging::init_log();
    log::info!("Starting nhagent...");

    let opt = Opts::from_args();
    log::info!("Opts: {:#?}", opt);

    let (tx, rx) = mpsc::channel();

    // let mut sampler = nhagent::sampler::OvsSampler::new(opt.interval_ms, tx);
    let mut sampler =
        nhagent::sampler::SsSampler::new(opt.interval_ms, opt.sampler_listen_port, tx);
    if !opt.disable_v2 {
        sampler.run();
    }

    log::info!("cluster: {}", CLUSTER.lock().unwrap().inner().to_dot());
    let my_role = CLUSTER.lock().unwrap().get_my_role();

    let mut comm = nhagent::communicator::Communicator::new(my_role)?;

    main_loop(rx, opt.interval_ms, &mut comm, &opt).unwrap();

    if !opt.disable_v2 {
        sampler.join().unwrap();
    }
    Ok(())
}

fn main_loop(
    rx: mpsc::Receiver<(TimeList, Vec<CounterUnit>)>,
    interval_ms: u64,
    comm: &mut Communicator,
    opt: &Opts,
) -> anyhow::Result<()> {
    log::info!("entering main loop");

    let mut handler = Handler::new(comm, interval_ms);

    use message::Message;
    {
        let msg = Message::DeclareEthHostTable(CLUSTER.lock().unwrap().eth_hostname().clone());
        log::debug!("broadcasting: {:?}", msg);
        comm.broadcast(msg)?;
    }
    {
        let msg = Message::DeclareIpHostTable(CLUSTER.lock().unwrap().ip_hostname().clone());
        log::debug!("broadcasting: {:?}", msg);
        comm.broadcast(msg)?;
    }
    {
        let msg = Message::DeclareHostname(hostname().clone());
        log::debug!("broadcasting: {:?}", msg);
        comm.broadcast(msg)?;
    }
    comm.barrier(0)?;
    log::debug!("barrier 0 passed");

    let poll = Rc::new(mio::Poll::new()?);
    comm.set_poll(Rc::clone(&poll));

    // cannot do here, must wait for all broadcast done
    // comm.reset_unnecessary_connections(&handler.rank_hostname)?;

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
        if now >= last_tp + interval && comm.bcast_done() && !opt.disable_v2 {
            // it's not very explicit why this is is correct or not, need more thinking on this
            handler.reset_traffic();

            // peroidically call the function to check if update is needed, if yes, then do it
            handler.update_background_flow_hard(comm)?;

            if comm.my_role() == Role::RackLeader || comm.my_role() == Role::GlobalLeader {
                handler.send_allhints(comm)?;
                handler.send_rack_chunk(comm)?;
            } else {
                handler.send_server_chunk(comm)?;
            }

            last_tp = now;
        }

        // poll data from sampler thread
        for (tl, v) in rx.try_iter() {
            for c in &v {
                log::trace!("counterunit: {:?}", c);
            }
            // receive from local sampler module
            handler.update_time_list(tl);
            handler.receive_server_chunk(comm, comm.my_rank(), v, None);
        }

        poll.poll(&mut events, Some(timeout))?;

        for event in events.iter() {
            let rank = event.token().0;
            // log::debug!("handle event from rank: {}", rank);

            if rank == comm.world_size() {
                // it must be the controller listener wants to accept new connections
                assert!(event.readiness().is_readable());
                comm.accept_app_connection()?;
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
                match ep.on_send_ready::<nhagent::message::Message>() {
                    Ok((_cmd, attachment)) => {
                        handler.on_send_complete(attachment)?;
                    }
                    Err(endpoint::Error::NothingToSend) => {
                        // deactivate to save CPU
                        poll.reregister(
                            ep.stream(),
                            event.token(),
                            ep.interest() - mio::Ready::writable(),
                            mio::PollOpt::level(),
                        )?;
                    }
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

    handler.cleanup();
    Ok(())
}

#[derive(Debug)]
enum CommandOp {
    Task(Command),
    Stop,
}

fn cmd_executor(rx: mpsc::Receiver<CommandOp>) -> anyhow::Result<()> {
    loop {
        let op = rx.recv().expect("cmd_executor recv error");
        match op {
            CommandOp::Task(cmd) => {
                utils::cmd_helper::get_command_output(cmd).unwrap();
            }
            CommandOp::Stop => {
                return Ok(());
            }
        }
    }
}

// service handler
struct Handler {
    traffic: HashMap<LinkIx, Vec<CounterUnit>>,
    // traffic that has been sent to other nodes
    committed_traffic: HashMap<LinkIx, Vec<CounterUnit>>,
    // traffic buffer for query use
    traffic_buf: HashMap<LinkIx, Vec<CounterUnit>>,
    rank_hostname: HashMap<usize, String>,
    brain: Rc<RefCell<Brain>>,
    interval_ms: u64,
    // (controller_rank, tenant_id, nhosts_to_acquire)
    provision_req_queue: VecDeque<(usize, TenantId, usize)>,
    // background flow hard
    background_flow_hard: BackgroundFlowHard,
    bfh_last_ts: std::time::Instant,
    // command executor thread to avoid blocking of main thread
    cmd_handle: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
    cmd_tx: mpsc::Sender<CommandOp>,
    // current time list
    time_list: TimeList,
    time_list_for_remote: TimeList,
    // time list for traffic buf
    time_list_buf: TimeList,
}

impl Handler {
    fn new(comm: &mut Communicator, interval_ms: u64) -> Self {
        let opts = Opts::from_args();
        use nhagent::cluster::*;
        let brain_setting = BrainSetting {
            seed: 1,
            asymmetric: false,
            broken: 0.0,
            max_slots: MAX_SLOTS,
            sharing_mode: nethint::SharingMode::Guaranteed,
            guaranteed_bandwidth: Some(opts.topo.host_bw() / MAX_SLOTS as f64),
            topology: opts.topo,
            background_flow_high_freq: Default::default(),
            gc_period: 100,
        };
        let brain = Brain::build_cloud(brain_setting);
        let (tx, rx) = mpsc::channel();
        let cmd_handle = Some(std::thread::spawn(|| cmd_executor(rx)));
        // make sure update background flow will succeed at the first call
        let bfh_last_ts = std::time::Instant::now()
            - std::time::Duration::from_nanos(opts.background_flow_hard.frequency_ns);
        Handler {
            traffic: HashMap::<LinkIx, Vec<CounterUnit>>::new(),
            committed_traffic: HashMap::<LinkIx, Vec<CounterUnit>>::new(),
            traffic_buf: HashMap::<LinkIx, Vec<CounterUnit>>::new(),
            rank_hostname: std::iter::once((comm.my_rank(), nhagent::cluster::hostname().clone()))
                .collect(),
            brain,
            interval_ms,
            provision_req_queue: VecDeque::new(),
            background_flow_hard: opts.background_flow_hard,
            bfh_last_ts,
            cmd_handle,
            cmd_tx: tx,
            time_list: TimeList::new(),
            time_list_for_remote: TimeList::new(),
            time_list_buf: TimeList::new(),
        }
    }

    fn update_time_list(&mut self, time_list: TimeList) {
        self.time_list.update_time_list(&time_list);
        self.time_list_for_remote.update_time_list(&time_list);
    }

    fn cleanup(&mut self) {
        self.cmd_tx.send(CommandOp::Stop).unwrap();
        self.cmd_handle.take().unwrap().join().unwrap().unwrap();
    }

    fn update_background_flow_hard(&mut self, comm: &mut Communicator) -> anyhow::Result<()> {
        if !self.background_flow_hard.enable || comm.my_role() != Role::GlobalLeader {
            return Ok(());
        }
        use std::time::*;
        let now = Instant::now();
        if now > self.bfh_last_ts + Duration::from_nanos(self.background_flow_hard.frequency_ns) {
            let bf = &self.background_flow_hard;
            self.brain.borrow_mut().update_background_flow_hard(
                bf.probability,
                bf.amplitude,
                bf.zipf_exp,
            );
            // for the egress traffic from the hosts, use mlnx_qos to limit the rates
            // for the ingress traffic to the hosts, use interface ethernet 1/x bandwidth shape 1.2G to limit the rate
            // ssh -oKexAlgorithms=+diffie-hellman-group14-sha1 danyang@danyang-mellanox-switch.cs.duke.edu cli -h '"enable" "config terminal" "interface ethernet 1/4 bandwidth shape 10G"'

            let brain = self.brain.borrow();
            let vc = brain.cluster();
            let mut switch_settings = Vec::new();
            switch_settings.push("enable".to_owned());
            switch_settings.push("config terminal".to_owned());

            let get_bw = |link_ix: LinkIx| {
                (vc[link_ix].bandwidth.val() as f64 / 1e9)
                    .round()
                    .clamp(1., 100.) as usize
            };

            let mut tor_bw = [100; 2];

            for link_ix in vc.all_links() {
                let n1 = &vc[vc.get_source(link_ix)];
                let n2 = &vc[vc.get_target(link_ix)];
                if !cluster::is_physical_node(n1) || !cluster::is_physical_node(n2) {
                    continue;
                }
                if n1.depth > n2.depth && n1.depth == 3 {
                    // egress traffic from the hosts
                    // ssh danyang-02 'mlnx_qos -i rdma0 -r 3,0,0,0,0,0,0,0'
                    // round to 1 Gbps precision
                    let bw = get_bw(link_ix);
                    let msg = nhagent::message::Message::UpdateRateLimit(bw);
                    let phys_hostname = cluster::vname_to_phys_hostname(&n1.name).unwrap();
                    let dst_rank = self
                        .rank_hostname
                        .iter()
                        .find_map(|(k, v)| if *v == phys_hostname { Some(*k) } else { None })
                        .unwrap();
                    if dst_rank == comm.my_rank() {
                        let mut cmd = Command::new("mlnx_qos");
                        cmd.args(&["-i", "rdma0", "-r", &format!("{},0,0,0,0,0,0,0", bw)]);
                        self.cmd_tx.send(CommandOp::Task(cmd)).unwrap();
                    } else {
                        comm.send_to(dst_rank, &msg)?;
                    }
                } else if n1.depth > n2.depth && n1.depth == 2 {
                    // egress traffic from the ToR switch
                    let tor_id: usize = n1.name.strip_prefix("tor_").unwrap().parse().unwrap();
                    assert!(tor_id <= 1, "unexpected tor_id: {} in testbed", tor_id);
                    let bw = get_bw(link_ix);
                    tor_bw[tor_id] = tor_bw[tor_id].min(bw);
                } else if n1.depth < n2.depth && n1.depth == 2 {
                    // ingress traffic to the hosts
                    let host_id: usize = n2.name.strip_prefix("host_").unwrap().parse().unwrap();
                    let bw = get_bw(link_ix);
                    // 0,1,2 => 4,6,8; 3,4,5 => 12,14,16
                    let table: [usize; 6] = [4, 6, 8, 12, 14, 16];
                    let eth_port = if host_id < 6 {
                        table[host_id]
                    } else {
                        panic!("unexpected tor_id: {} in testbed", host_id);
                    };
                    let cmd = format!("interface ethernet 1/{} bandwidth shape {}G", eth_port, bw);
                    switch_settings.push(cmd);
                } else if n1.depth == 1 && n2.depth == 2 {
                    // because we only have 2 racks in the testbed, the receiving rate limitation can be translate to the sending rate limitation
                    let tor_id: usize = n2.name.strip_prefix("tor_").unwrap().parse().unwrap();
                    assert!(tor_id <= 1, "unexpected tor_id: {} in testbed", tor_id);
                    let bw = get_bw(link_ix);
                    tor_bw[tor_id ^ 1] = tor_bw[tor_id ^ 1].min(bw);
                } else {
                    // we may omit other cases for now, as there is no direct support for this case
                }
            }

            for (&bw, &eth_port) in tor_bw.iter().zip(&["1/2", "1/10"]) {
                let cmd = format!("interface ethernet {} bandwidth shape {}G", eth_port, bw);
                switch_settings.push(cmd);
            }

            // update switch settings once for all
            let enclose_with = |s: &mut String, ch: char| {
                s.insert(0, ch);
                s.push(ch);
            };
            let mut cmd = Command::new("ssh");
            switch_settings
                .iter_mut()
                .for_each(|s| enclose_with(s, '"'));
            let switch_setting_str = switch_settings.join(" ");
            // enclose_with(&mut switch_setting_str, '\'');
            cmd.args(&[
                "-oKexAlgorithms=+diffie-hellman-group14-sha1",
                "danyang@danyang-mellanox-switch.cs.duke.edu",
                "cli",
                "-h",
                &switch_setting_str,
            ]);
            self.cmd_tx.send(CommandOp::Task(cmd)).unwrap();

            self.bfh_last_ts = Instant::now();
        }
        Ok(())
    }

    fn merge_chunk(chunk1: &Vec<CounterUnit>, chunk2: &Vec<CounterUnit>) -> Vec<CounterUnit> {
        // should we really add num_competitors together here?
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

    fn commit_chunk(chunk1: &mut Vec<CounterUnit>, chunk2: &mut Vec<CounterUnit>) {
        use std::collections::BTreeMap;
        let mut chunk2_map: BTreeMap<String, CounterUnit> = chunk2
            .into_iter()
            .map(|x| (x.vnodename.clone(), x.clone()))
            .collect();
        for c in chunk1 {
            chunk2_map.entry(c.vnodename.clone()).and_modify(|e| {
                c.subtract(e);
                e.clear();
            });
        }
        *chunk2 = chunk2_map.into_values().collect();
    }

    fn reset_traffic(&mut self) {
        // self.traffic = Default::default();
        // flush to traffic buf
        let links: Vec<LinkIx> = self.traffic.keys().copied().collect();
        self.update_traffic_buf_iter(links.iter());
        for (&k, v) in &mut self.committed_traffic {
            self.traffic
                .entry(k)
                .and_modify(|e| Self::commit_chunk(e, v));
        }
        // self.time_list_buf = self.time_list.clone();
        // self.time_list.clear();
    }

    fn merge_traffic(
        traffic: &mut HashMap<LinkIx, Vec<CounterUnit>>,
        chunks: HashMap<LinkIx, Vec<CounterUnit>>,
    ) {
        // 1. parse chunk, aggregate
        for (l, c) in chunks {
            Self::merge_traffic_on_link(traffic, l, c);
        }
    }

    fn merge_traffic_on_link(
        traffic: &mut HashMap<LinkIx, Vec<CounterUnit>>,
        link_ix: LinkIx,
        chunk: Vec<CounterUnit>,
    ) {
        // insert or merge
        if traffic.contains_key(&link_ix) {
            *traffic.get_mut(&link_ix).unwrap() = Self::merge_chunk(&traffic[&link_ix], &chunk);
        } else {
            traffic.insert(link_ix, chunk);
        }
    }

    fn update_traffic_buf(&mut self, link_ix: LinkIx) {
        self.traffic_buf
            .insert(link_ix, self.traffic[&link_ix].clone());
    }

    fn update_traffic_buf_iter<'a>(&mut self, iter: impl Iterator<Item = &'a LinkIx>) {
        for &link_ix in iter {
            self.update_traffic_buf(link_ix);
        }
        // also update time_list_buf
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
                if pcluster.get_role(&*h) != Role::Worker
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
                let mut time_list = TimeList::new();
                if let Some(tr) = self.time_list_for_remote.get(timing::ON_COLLECTED) {
                    time_list.push(&tr.stage, tr.ts);
                }
                if let Some(tr) = self.time_list_for_remote.get(timing::ON_SAMPLED) {
                    time_list.push(&tr.stage, tr.ts);
                };
                let chunk = self.traffic[&uplink].clone();
                let msg = message::Message::ServerChunk(chunk.clone(), time_list);
                comm.send_to(my_rack_leader_rank.unwrap(), &msg)?;

                // commit the sent data by insertion or merge
                Self::merge_traffic_on_link(&mut self.committed_traffic, uplink, chunk);
                self.time_list.update_now(timing::ON_CHUNK_SENT);
            }
        }

        Ok(())
    }

    fn receive_server_chunk(&mut self, comm: &mut Communicator, sender_rank: usize, chunk: Vec<CounterUnit>, remote_time_list: Option<TimeList>) {
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
        // 3. update traffic
        Self::merge_traffic_on_link(&mut self.traffic, uplink, chunk);

        // update time list, for GlobalLeader or RackLeader
        let my_role = comm.my_role();
        if my_role == Role::GlobalLeader || my_role == Role::RackLeader {
            self.time_list.update_now(timing::ON_CHUNK_SENT);
        } else {
            if let Some(remote_time_list) = remote_time_list {
                self.time_list.update_min(timing::ON_COLLECTED, &remote_time_list);
                self.time_list.update_min(timing::ON_SAMPLED, &remote_time_list);
            }
        }
    }

    // Assume LinkIx from different agents are compatible with each other
    fn receive_rack_chunk(&mut self, chunks: HashMap<LinkIx, Vec<CounterUnit>>, remote_time_list: TimeList) {
        Self::merge_traffic(&mut self.traffic, chunks);
        self.time_list.update_min(timing::ON_COLLECTED, &remote_time_list);
        self.time_list.update_min(timing::ON_SAMPLED, &remote_time_list);
    }

    fn send_rack_chunk(&mut self, comm: &mut Communicator) -> anyhow::Result<()> {
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
                .ok_or(())
                .unwrap_err();
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
            .ok_or(())
            .unwrap_err();
        // 2. send to all other rack leaders
        let mut time_list = TimeList::new();
        if let Some(tr) = self.time_list_for_remote.get(timing::ON_COLLECTED) {
            time_list.push(&tr.stage, tr.ts);
        };
        if let Some(tr) = self.time_list_for_remote.get(timing::ON_SAMPLED) {
            time_list.push(&tr.stage, tr.ts);
        };
        let msg = message::Message::RackChunk(my_rack_traffic, time_list);
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
        // 3. commit the sent traffic. actually, it is already committed in send_allhints
        // Self::merge_traffic(&mut self.committed_traffic, my_rack_traffic);
        Ok(())
    }

    fn send_allhints(&mut self, comm: &mut Communicator) -> anyhow::Result<()> {
        assert!(comm.my_role() == Role::RackLeader || comm.my_role() == Role::GlobalLeader);
        let pcluster = CLUSTER.lock().unwrap();
        let chunk: HashMap<LinkIx, Vec<CounterUnit>> =
            self.traffic.iter().map(|(&k, v)| (k, v.clone())).collect();
        let msg = message::Message::AllHints(chunk.clone());
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
                log::trace!("send_allhints, peer_rank: {}, peer_hostname: {}", i, peer_hostname);
                comm.send_to(i, &msg)?;
            }
        }
        // 3. commit the sent traffic
        Self::merge_traffic(&mut self.committed_traffic, chunk);

        // update time list, for GlobalLeader or RackLeader
        let my_role = pcluster.get_my_role();
        if my_role == Role::GlobalLeader || my_role == Role::RackLeader {
            self.time_list.update_now(timing::ON_ALL_RECEIVED);
            self.time_list_buf = self.time_list.clone();
            self.time_list.clear();
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
        let my_node_ix = pcluster.my_node_ix();
        let uplink = pcluster.inner().get_uplink(my_node_ix);
        for (l, c) in allhints {
            if l != uplink {
                // insert or overwrite
                self.traffic.insert(l, c);
            }
        }
        self.time_list.update_now(timing::ON_ALL_RECEIVED);
        self.time_list_buf = self.time_list.clone();
        self.time_list.clear();
        log::debug!("worker agent link traffic: {:?}", self.traffic);
        Ok(())
    }

    fn handle_provision(
        &mut self,
        sender_rank: usize,
        comm: &mut Communicator,
        tenant_id: TenantId,
        nhosts_to_acquire: usize,
        allow_delay: bool,
    ) -> anyhow::Result<()> {
        log::info!(
            "handle_provision: {} {} {} {}",
            sender_rank,
            tenant_id,
            nhosts_to_acquire,
            allow_delay
        );
        use message::Message::*;
        // connect the code to Brain, provision VMs, return the vcluster
        match self.brain.borrow_mut().provision(
            tenant_id,
            nhosts_to_acquire,
            PlacementStrategy::Compact,
        ) {
            Ok(vc) => {
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
            Err(e @ nethint::brain::Error::NoHost(_, _)) if allow_delay => {
                log::warn!("delaying the request because of the provision error: {}", e);
                return Err(anyhow::anyhow!("delaying the request"));
            }
            Err(e) => {
                panic!("provision error: {}", e);
            }
        }
        Ok(())
    }

    fn delay_provision(&mut self, comm: &mut Communicator) -> anyhow::Result<()> {
        log::info!("delay_provision");
        while let Some(&(sender_rank, tid, nhosts)) = self.provision_req_queue.front() {
            self.handle_provision(sender_rank, comm, tid, nhosts, true)?;
            self.provision_req_queue.pop_front();
        }
        Ok(())
    }

    fn get_vname_to_hostname(vc: &VirtCluster) -> HashMap<String, String> {
        (0..vc.num_hosts())
            .map(|vid| {
                // 0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 8, 5 -> 9
                let vname = format!("host_{}", vid);
                let pname = vc.virt_to_phys()[&vname].clone();
                let pid: usize = pname.strip_prefix("host_").unwrap().parse().unwrap();
                let vm_local_id = vc.virt_to_vmno()[&vname];
                let vmid = pid * nhagent::cluster::MAX_SLOTS + vm_local_id;
                let hostid = (vmid / 4) * 8 + vmid % 4;
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
        log::trace!(
            "on_recv_complete, sender_rank: {} msg: {:?}",
            sender_rank,
            msg
        );
        match msg {
            AppFinish => {
                TERMINATE.store(true, SeqCst);
            }
            BcastMessage(bcast_id, msg) => {
                comm.recv_bcast_msg(bcast_id);
                self.on_recv_complete(*msg, sender_rank, comm)?;
                if comm.bcast_done() {
                    log::trace!("rank_hostname: {:#?}", self.rank_hostname);
                    comm.reset_unnecessary_connections(&self.rank_hostname)?;
                }
            }
            DeclareEthHostTable(table) => {
                // merge table from other hosts
                let mut pcluster = CLUSTER.lock().unwrap();
                pcluster.update_eth_hostname(table);
            }
            DeclareIpHostTable(table) => {
                // merge table from other hosts
                let mut pcluster = CLUSTER.lock().unwrap();
                pcluster.update_ip_hostname(table);
            }
            DeclareHostname(hostname) => {
                log::trace!("received DeclareHostname, sender_rank: {}, hostname: {}", sender_rank, hostname);
                self.rank_hostname.insert(sender_rank, hostname);
            }
            ServerChunk(chunk, remote_time_list) => {
                let my_role = comm.my_role();
                assert!(my_role == Role::RackLeader || my_role == Role::GlobalLeader);
                self.receive_server_chunk(comm, sender_rank, chunk, Some(remote_time_list));
            }
            RackChunk(chunk, remote_time_list) => {
                let my_role = comm.my_role();
                assert_ne!(my_role, Role::Worker);
                self.receive_rack_chunk(chunk, remote_time_list);
                log::trace!("rack leader agent link traffic: {:?}", self.traffic);
            }
            AllHints(allhints) => {
                log::trace!("receive AllHints: sender_rank: {}", sender_rank);
                self.receive_allhints(allhints)?;
            }
            // serve as cloud brain, by global leader
            ProvisionRequest(tenant_id, nhosts_to_acquire, allow_delay) => {
                if allow_delay {
                    self.provision_req_queue
                        .push_back((sender_rank, tenant_id, nhosts_to_acquire));
                    self.delay_provision(comm).unwrap_or_default();
                } else {
                    self.handle_provision(sender_rank, comm, tenant_id, nhosts_to_acquire, false)?;
                }
            }
            DestroyRequest(tenant_id) => {
                // destroy and release the resource immediately in testbed
                self.brain.borrow_mut().destroy(tenant_id);
                self.brain.borrow_mut().garbage_collect(tenant_id + 1);
                let msg = DestroyResponse(tenant_id);
                comm.send_to(sender_rank, &msg)?;

                self.delay_provision(comm).unwrap_or_default();
            }
            NetHintRequest(tenant_id, version, req_time_list) => {
                match version {
                    NetHintVersion::V1 => {
                        let vc = (*self.brain.borrow().vclusters()[&tenant_id].borrow()).clone();
                        let vname_to_hostname = Self::get_vname_to_hostname(&vc);
                        let hintv1 = NetHintV1Real {
                            vc,
                            vname_to_hostname,
                        };
                        let msg = NetHintResponseV1(tenant_id, hintv1);
                        comm.send_to(sender_rank, &msg)?;
                    }
                    NetHintVersion::V2 => {
                        // handle time list
                        let mut time_list = self.time_list_buf.clone();
                        time_list.update_time_list(&req_time_list);
                        time_list.update_now(timing::ON_RECV_TENANT_REQ);
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
                            if let Some(traffic_on_link) = self.traffic_buf.get(&phys_link) {
                                traffic.insert(vlink_ix, traffic_on_link.clone());
                            }
                            // set vc.bandwidth to the value of physical links
                            vc[vlink_ix].bandwidth =
                                self.brain.borrow().cluster()[phys_link].bandwidth;
                        }
                        // we need to do some modifications to traffic_on_link
                        // because it contains all traffic from all tenants
                        // the traffic from the requestor itself must be subtracted
                        for vlink_ix in vc.all_links() {
                            if !traffic.contains_key(&vlink_ix) {
                                continue;
                            }
                            let n1 = &vc[vc.get_source(vlink_ix)];
                            if n1.depth == 3 {
                                let vm_local_id = vc.virt_to_vmno()[&n1.name].to_string();
                                let traffic_on_link = traffic.get_mut(&vlink_ix).unwrap();
                                if let Some(pos) = traffic_on_link
                                    .iter()
                                    .position(|c| c.vnodename == vm_local_id)
                                {
                                    let c = traffic_on_link.remove(pos);
                                    // also update rack uplink
                                    let rack_uplink = vc.get_uplink(vc.get_target(vlink_ix));
                                    traffic.entry(rack_uplink).and_modify(|dataunit| {
                                        use nethint::counterunit::CounterType::*;
                                        let tx_out = c.data[Tx] - c.data[TxIn];
                                        let rx_out = c.data[Rx] - c.data[RxIn];
                                        log::debug!(
                                            "node: {}, dataunit[0].data[Tx]: {:?}, tx_out: {:?}",
                                            n1.name,
                                            dataunit[0].data[Tx],
                                            tx_out
                                        );
                                        log::debug!(
                                            "node: {}, dataunit[0].data[Rx]: {:?}, rx_out: {:?}",
                                            n1.name,
                                            dataunit[0].data[Rx],
                                            rx_out
                                        );
                                        assert!(
                                            dataunit[0].data[Tx].bytes >= tx_out.bytes,
                                            "{:?} vs {:?}, node: {}",
                                            dataunit[0].data[Tx],
                                            tx_out,
                                            n1.name
                                        );
                                        assert!(
                                            dataunit[0].data[Rx].num_competitors
                                                >= rx_out.num_competitors,
                                            "{:?} vs {:?}, node: {}",
                                            dataunit[0].data[Rx],
                                            rx_out,
                                            n1.name
                                        );
                                        dataunit[0].data[Tx] -= tx_out;
                                        dataunit[0].data[Rx] -= rx_out;
                                    });
                                }
                            }
                        }
                        let vname_to_hostname = Self::get_vname_to_hostname(&vc);
                        let hintv1 = NetHintV1Real {
                            vc,
                            vname_to_hostname,
                        };
                        let hintv2 = NetHintV2Real {
                            hintv1,
                            interval_ms: self.interval_ms,
                            traffic,
                        };
                        let msg = NetHintResponseV2(tenant_id, hintv2, time_list);
                        comm.send_to(sender_rank, &msg)?;
                    }
                }
            }
            UpdateRateLimit(bw) => {
                let mut cmd = Command::new("mlnx_qos");
                cmd.args(&["-i", "rdma0", "-r", &format!("{},0,0,0,0,0,0,0", bw)]);
                self.cmd_tx.send(CommandOp::Task(cmd)).unwrap();
                // let output = utils::cmd_helper::get_command_output(cmd);
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
