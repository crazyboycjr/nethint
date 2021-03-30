#![feature(option_unwrap_none)]
#![feature(new_uninit)]

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

use replayer::message;
use replayer::Node;

use litemsg::endpoint;
use litemsg::endpoint::Endpoint;

static TERMINATE: AtomicBool = AtomicBool::new(false);

fn partition(mut peers: Vec<Endpoint>, num_groups: usize) -> Vec<Vec<Endpoint>> {
    let mut groups = Vec::with_capacity(num_groups);
    groups.resize_with(num_groups, Vec::new);
    for (i, s) in peers.drain(..).enumerate() {
        groups[i % num_groups].push(s);
    }
    groups
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Operation {
    RECV,
    SEND,
    STOP,
}

struct WorkRequest {
    op: Operation,
    rank: usize,
    ep: Box<Endpoint>,
}

enum Completion {
    SendComplete(usize, Box<Endpoint>, Box<Option<Vec<u8>>>),
    RecvComplete(usize, Box<Endpoint>, Box<message::Command>),
    ConnectionLost(usize, Box<Endpoint>),
    WouldBlock(usize, Box<Endpoint>),
    NothingToSend(usize, Box<Endpoint>),
    WorkerThreadExit,
}

fn main() -> anyhow::Result<()> {
    logging::init_log();

    let controller_uri = std::env::var("RP_CONTROLLER_URI").expect("RP_CONTROLLER_URI");
    log::info!("connecting to controller_uri: {}", controller_uri);

    let (mut nodes, my_node, controller, mut listener) =
        litemsg::connect_controller(&controller_uri, 7)?;
    let mut controller_ep = endpoint::Builder::new()
        .stream(controller)
        .readable(true)
        .writable(true)
        .node(controller_uri.parse().unwrap())
        .build()
        .unwrap();

    let my_rank = litemsg::get_my_rank(&my_node, &nodes);

    let mut peers = litemsg::connect_peers2(&nodes, &my_node, &mut listener)?;
    nodes.reverse();
    let mut peers2 = litemsg::connect_peers2(&nodes, &my_node, &mut listener)?;
    peers.append(&mut peers2);
    // build endpoint and wrap them in Arc<Mutex<>>
    let peers: Vec<Option<Box<Endpoint>>> = peers
        .into_iter()
        .map(|b| Some(Box::new(b.build().unwrap())))
        .collect();

    // start IO threads, another half for kthreads running network stack
    let io_threads = std::cmp::max(1, (num_cpus::get() / 2) - 1);
    // let mut peers_group = partition(peers, io_threads);

    // completion queue of io worker thread
    let (comp_tx, comp_rx) = mpsc::channel();
    let mut work_tx = Vec::new();

    let mut handles = vec![];
    for _ in 0..io_threads {
        // let peers = std::mem::take(&mut peers_group[i]);

        let comp_tx = comp_tx.clone();
        // work queue
        let (tx, rx) = mpsc::channel();
        work_tx.push(tx);
        let handle = std::thread::spawn(move || {
            io_worker(rx, comp_tx).unwrap();
        });
        handles.push(handle);
    }

    EventLoop::new(
        io_threads,
        my_rank,
        peers,
        &mut controller_ep,
        work_tx,
        comp_rx,
    )
    .run()?;

    for h in handles {
        h.join().unwrap();
    }

    // send LeaveNode
    log::info!("worker is leaving the group");
    controller_ep.post(message::Command::LeaveNode(my_node), None)?;
    loop {
        match controller_ep.on_send_ready() {
            Ok(_) => break,
            Err(endpoint::Error::NothingToSend) => {}
            Err(endpoint::Error::WouldBlock) => {}
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    log::info!("worker left");

    Ok(())
}

fn io_worker(wq: Receiver<WorkRequest>, cq: Sender<Completion>) -> anyhow::Result<()> {
    log::info!("io worker started");

    loop {
        let wr = wq.recv()?;
        let rank = wr.rank;
        let mut ep = wr.ep;
        let comp = match wr.op {
            Operation::RECV => {
                match ep.on_recv_ready() {
                    Ok((cmd, _)) => Completion::RecvComplete(rank, ep, Box::new(cmd)),
                    Err(endpoint::Error::WouldBlock) => Completion::WouldBlock(rank, ep),
                    Err(endpoint::Error::ConnectionLost) => {
                        Completion::ConnectionLost(rank, ep)
                        // poll.deregister(ep.stream()).unwrap();
                    }
                    Err(e) => {
                        // unexpected error, exit the io worker
                        return Err(e.into());
                    }
                }
            }
            Operation::SEND => {
                match ep.on_send_ready() {
                    Ok(attachment) => Completion::SendComplete(rank, ep, Box::new(attachment)),
                    Err(endpoint::Error::NothingToSend) => Completion::NothingToSend(rank, ep),
                    Err(endpoint::Error::WouldBlock) => Completion::WouldBlock(rank, ep),
                    Err(e) => {
                        // unexpected error, exit the io worker
                        return Err(e.into());
                    }
                }
            }
            Operation::STOP => {
                cq.send(Completion::WorkerThreadExit)?;
                break;
            }
        };

        cq.send(comp)?;
    }

    Ok(())
}

struct EventLoop<'a> {
    io_threads: usize,
    _my_rank: usize,
    peers: Vec<Option<Box<Endpoint>>>,
    controller_ep: &'a mut Endpoint,
    wqs: Vec<Sender<WorkRequest>>,
    cq: Option<Receiver<Completion>>,

    meter: Meter,
    peer_index: HashMap<Node, usize>, // index into peers vec
    // in case that dst_ep is not currently available, we buffer the post request, the key is a index into peers
    post_buffer: Vec<VecDeque<replayer::Flow>>,
}

impl<'a> EventLoop<'a> {
    fn new(
        io_threads: usize,
        my_rank: usize,
        peers: Vec<Option<Box<Endpoint>>>,
        controller_ep: &'a mut Endpoint,
        wqs: Vec<Sender<WorkRequest>>,
        cq: Receiver<Completion>,
    ) -> Self {
        let mut peer_index: HashMap<Node, usize> = HashMap::new();
        for (i, ep) in peers.iter().enumerate() {
            if ep.as_ref().unwrap().interest().is_writable() {
                peer_index.insert(ep.as_ref().unwrap().node().clone(), i);
            }
        }

        let mut post_buffer = Vec::with_capacity(peers.len());
        post_buffer.resize_with(peers.len(), || Default::default());

        EventLoop {
            io_threads,
            _my_rank: my_rank,
            peers,
            controller_ep,
            wqs,
            cq: Some(cq),
            meter: Meter::new(my_rank),
            peer_index,
            post_buffer,
        }
    }

    fn on_send_complete(&mut self, attachment: Option<Vec<u8>>) -> anyhow::Result<()> {
        // count the sending rate
        let len = attachment.map(|a| a.len()).unwrap_or(0);
        if len > 0 {
            self.meter.add_bytes(len as _);
        }
        Ok(())
    }

    fn on_recv_complete(&mut self, cmd: message::Command) -> anyhow::Result<()> {
        use message::Command::*;
        match cmd {
            EmitFlow(flow) => {
                log::info!("prepare to start a flow: {:?}", flow);
                let rank = self.peer_index[&flow.dst];
                self.post_buffer[rank].push_back(flow);
            }
            AppFinish => {
                TERMINATE.store(true, SeqCst);
            }
            Data(flow) => {
                // flow received, notify controller with FlowComplete
                let msg = FlowComplete(flow);
                self.controller_ep.post(msg, None)?;
            }
            _ => {
                log::error!("handle_msg: unexpected cmd: {:?}", cmd);
            }
        }

        Ok(())
    }

    fn run(&mut self) -> anyhow::Result<()> {
        log::info!(
            "entering event loop, number of Endpoints: {}",
            self.peers.len()
        );

        if self.peers.is_empty() {
            return Ok(());
        }

        let poll = mio::Poll::new()?;

        let mut events = mio::Events::with_capacity(256);

        for (i, ep) in self.peers.iter().enumerate() {
            log::trace!(
                "registering ep.interest: {:?}",
                ep.as_ref().unwrap().interest()
            );
            let interest = ep.as_ref().unwrap().interest();
            poll.register(
                ep.as_ref().unwrap().stream(),
                mio::Token(i),
                interest,
                mio::PollOpt::level(),
            )?;
        }

        poll.register(
            self.controller_ep.stream(),
            mio::Token(self.peers.len()),
            self.controller_ep.interest(),
            mio::PollOpt::level(),
        )?;

        // steer flow to a particular io thread
        let mut flow_worker_id = Vec::with_capacity(self.peers.len());
        for (i, _ep) in self.peers.iter().enumerate() {
            flow_worker_id.push(i % self.io_threads);
        }

        let cq = self.cq.take().unwrap();

        let timeout = std::time::Duration::from_micros(1);

        while !TERMINATE.load(SeqCst) {
            poll.poll(&mut events, Some(timeout))?;
            for event in events.iter() {
                let rank = event.token().0;
                assert!(rank <= self.peers.len());

                if event.readiness().is_writable() {
                    if rank < self.peers.len() {
                        if let Some(ep) = self.peers[rank].take() {
                            let tid = flow_worker_id[rank];
                            self.wqs[tid].send(WorkRequest {
                                rank,
                                ep,
                                op: Operation::SEND,
                            })?;
                        } // else ignore this event because we're using level trigger
                    } else {
                        match self.controller_ep.on_send_ready() {
                            Ok(attachment) => {
                                self.on_send_complete(attachment)?;
                            }
                            Err(endpoint::Error::NothingToSend) => {}
                            Err(endpoint::Error::WouldBlock) => {}
                            Err(e) => {
                                return Err(e.into());
                            }
                        }
                    }
                }
                if event.readiness().is_readable() {
                    if rank < self.peers.len() {
                        if let Some(ep) = self.peers[rank].take() {
                            let tid = flow_worker_id[rank];
                            self.wqs[tid].send(WorkRequest {
                                rank,
                                ep,
                                op: Operation::RECV,
                            })?;
                        }
                    } else {
                        match self.controller_ep.on_recv_ready() {
                            Ok((cmd, _)) => {
                                self.on_recv_complete(cmd)?;
                            }
                            Err(endpoint::Error::WouldBlock) => {}
                            Err(endpoint::Error::ConnectionLost) => {
                                // looks strange here, then what is the next step?
                                poll.deregister(self.controller_ep.stream()).unwrap();
                            }
                            Err(e) => {
                                return Err(e.into());
                            }
                        }
                    }
                }
            }

            // poll cq
            for comp in cq.try_iter() {
                use Completion::*;
                match comp {
                    SendComplete(rank, ep, attachment) => {
                        self.peers[rank].replace(ep);
                        self.on_send_complete(*attachment)?;
                    }
                    RecvComplete(rank, ep, cmd) => {
                        self.peers[rank].replace(ep);
                        self.on_recv_complete(*cmd)?;
                    }
                    ConnectionLost(_rank, ep) => {
                        poll.deregister(ep.stream()).unwrap();
                    }
                    WouldBlock(rank, ep) => {
                        self.peers[rank].replace(ep);
                    }
                    NothingToSend(rank, ep) => {
                        // because there is nothing to send for now, we temporarily deregister this handle from epoll
                        // but we do not use deregister. Instead, we use reregister with an empty interest for efficiency (?)
                        assert_eq!(ep.interest(), mio::Ready::writable());
                        poll.reregister(
                            ep.stream(),
                            mio::Token(rank),
                            mio::Ready::empty(),
                            mio::PollOpt::level(),
                        )?;
                        self.peers[rank].replace(ep);
                    }
                    WorkerThreadExit => {
                        panic!("unexpected WorkerThreadExit");
                    }
                }
            }

            // clear post buffers is possible
            for rank in 0..self.peers.len() {
                if let Some(dst_ep) = self.peers[rank].as_mut() {
                    let mut has_new_data = false;
                    while let Some(flow) = self.post_buffer[rank].pop_front() {
                        has_new_data = true;
                        log::info!("start a flow: {:?}", flow);
                        let mut data = Vec::with_capacity(flow.bytes);
                        unsafe {
                            data.set_len(flow.bytes);
                        }
                        let msg = message::Command::Data(flow);
                        dst_ep.post(msg, Some(data))?;
                    }
                    if has_new_data {
                        // because there's new data to send, so we reactivate the ep
                        assert_eq!(dst_ep.interest(), mio::Ready::writable());
                        poll.reregister(
                            dst_ep.stream(),
                            mio::Token(rank),
                            dst_ep.interest(),
                            mio::PollOpt::level(),
                        )?;
                    }
                }
            }
        }

        for i in 0..self.io_threads {
            self.wqs[i].send(WorkRequest {
                rank: 0,
                ep: unsafe { Box::new_zeroed().assume_init() },
                op: Operation::STOP,
            })?;
        }

        for comp in cq.iter() {
            if matches!(comp, Completion::WorkerThreadExit) {
                log::info!("a worker thread is finished");
                self.io_threads -= 1;
            }
            if self.io_threads == 0 {
                break;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Meter {
    accumulated: isize,
    last_tp: std::time::Instant,
    refresh_interval: std::time::Duration,
    my_rank: usize,
}

impl Meter {
    fn new(my_rank: usize) -> Self {
        Meter {
            accumulated: 0,
            last_tp: std::time::Instant::now(),
            refresh_interval: std::time::Duration::new(1, 0),
            my_rank,
        }
    }

    #[inline]
    fn add_bytes(&mut self, delta: isize) {
        self.accumulated += delta;
        let now = std::time::Instant::now();
        if now - self.last_tp >= self.refresh_interval {
            println!(
                "Rank: {}, Speed: {} Gb/s",
                self.my_rank,
                8e-9 * self.accumulated as f64 / (now - self.last_tp).as_secs_f64()
            );
            self.accumulated = 0;
            self.last_tp = now;
        }
    }
}
