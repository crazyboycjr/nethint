#![feature(option_unwrap_none)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

use replayer::message;
use replayer::Node;

use litemsg::endpoint;

type Endpoint = Arc<Mutex<endpoint::Endpoint>>;

static TERMINATE: AtomicBool = AtomicBool::new(false);

fn partition(mut peers: Vec<Endpoint>, num_groups: usize) -> Vec<Vec<Endpoint>> {
    let mut groups = Vec::with_capacity(num_groups);
    groups.resize_with(num_groups, Vec::new);
    for (i, s) in peers.drain(..).enumerate() {
        groups[i % num_groups].push(s);
    }
    groups
}

fn main() -> anyhow::Result<()> {
    logging::init_log();

    let controller_uri = std::env::var("RP_CONTROLLER_URI").expect("RP_CONTROLLER_URI");
    log::info!("connecting to controller_uri: {}", controller_uri);

    let (mut nodes, my_node, controller, mut listener) =
        litemsg::connect_controller(&controller_uri)?;
    let controller_ep = Arc::new(Mutex::new(
        endpoint::Builder::new()
            .stream(controller)
            .readable(true)
            .writable(true)
            .node(controller_uri.parse().unwrap())
            .build()
            .unwrap(),
    ));

    let mut peers = litemsg::connect_peers2(&nodes, &my_node, &mut listener)?;
    nodes.reverse();
    let mut peers2 = litemsg::connect_peers2(&nodes, &my_node, &mut listener)?;
    peers.append(&mut peers2);
    // build endpoint and wrap them in Arc<Mutex<>>
    let peers: Vec<Endpoint> = peers
        .into_iter()
        .map(|b| Arc::new(Mutex::new(b.build().unwrap())))
        .collect();

    let handler = Arc::new(Mutex::new(Handler::new(
        Arc::clone(&controller_ep),
        peers.clone(),
    ))); // peers.clone is to call Arc::clone() on the elements

    // start IO threads
    let io_threads = std::cmp::max(1, num_cpus::get() - 1);
    let mut peers_group = partition(peers, io_threads);

    let mut handles = vec![];
    for i in 0..io_threads {
        let peers = std::mem::take(&mut peers_group[i]);
        let handler = Arc::clone(&handler);
        let handle = std::thread::spawn(move || {
            io_loop(handler, peers).unwrap();
        });
        handles.push(handle);
    }

    // start a process only drives controller in main thread
    io_loop(Arc::clone(&handler), vec![Arc::clone(&controller_ep)]).unwrap();

    for h in handles {
        h.join().unwrap();
    }

    // send LeaveNode
    controller_ep
        .lock()
        .unwrap()
        .post(message::Command::LeaveNode(my_node), None)?;
    loop {
        match controller_ep.lock().unwrap().on_send_ready() {
            Ok(_) => break,
            Err(endpoint::Error::NothingToSend) => {}
            Err(endpoint::Error::WouldBlock) => {}
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn io_loop(handler: Arc<Mutex<Handler>>, peers: Vec<Endpoint>) -> anyhow::Result<()> {
    log::info!("entering io looping, number of Endpoints: {}", peers.len());

    if peers.is_empty() {
        return Ok(());
    }

    let poll = mio::Poll::new()?;

    let mut events = mio::Events::with_capacity(256);

    for (i, ep) in peers.iter().enumerate() {
        log::trace!("registering ep.interest: {:?}", ep.lock().unwrap().interest());
        let interest = ep.lock().unwrap().interest();
        poll.register(
            ep.lock().unwrap().stream(),
            mio::Token(i),
            interest,
            mio::PollOpt::level(),
        )?;
    }

    let timeout = std::time::Duration::from_millis(1);

    while !TERMINATE.load(SeqCst) {
        poll.poll(&mut events, Some(timeout))?;
        for event in events.iter() {
            assert!(event.token().0 <= peers.len());

            let mut ep = peers[event.token().0].lock().unwrap();

            if event.readiness().is_writable() {
                match ep.on_send_ready() {
                    Ok(attachment) => {
                        handler.lock().unwrap().on_send_complete(attachment)?;
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
                        // to prevent circular wait
                        std::mem::drop(ep);
                        handler.lock().unwrap().on_recv_complete(cmd)?;
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

struct Handler {
    controller: Endpoint,
    meter: Meter,
    peer_table: HashMap<Node, Endpoint>,
}

impl Handler {
    fn new(controller: Endpoint, peers: Vec<Endpoint>) -> Self {
        let mut peer_table: HashMap<Node, Endpoint> = HashMap::new();
        for ep in peers {
            if ep.lock().unwrap().interest().is_writable() {
                peer_table.insert(ep.lock().unwrap().node().clone(), Arc::clone(&ep));
            }
        }

        Handler {
            controller,
            meter: Meter::new(),
            peer_table,
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
                let dst_ep = self.peer_table.get_mut(&flow.dst).unwrap();
                let mut data = Vec::with_capacity(flow.bytes);
                unsafe {
                    data.set_len(flow.bytes);
                }
                let msg = Data(flow);
                dst_ep.lock().unwrap().post(msg, Some(data))?;
            }
            AppFinish => {
                TERMINATE.store(true, SeqCst);
            }
            Data(flow) => {
                // flow received, notify controller with FlowComplete
                let msg = FlowComplete(flow);
                self.controller.lock().unwrap().post(msg, None)?;
            }
            _ => {
                log::error!("handle_msg: unexpected cmd: {:?}", cmd);
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
}

impl Meter {
    fn new() -> Self {
        Meter {
            accumulated: 0,
            last_tp: std::time::Instant::now(),
            refresh_interval: std::time::Duration::new(1, 0),
        }
    }

    #[inline]
    fn add_bytes(&mut self, delta: isize) {
        self.accumulated += delta;
        let now = std::time::Instant::now();
        if now - self.last_tp >= self.refresh_interval {
            println!(
                "Speed: {} Gb/s",
                8e-9 * self.accumulated as f64 / (now - self.last_tp).as_secs_f64()
            );
            self.accumulated = 0;
            self.last_tp = now;
        }
    }
}
