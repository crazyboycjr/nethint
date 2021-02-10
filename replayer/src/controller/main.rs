use std::collections::HashMap;
use std::io::{Read, Write};

use replayer::mapreduce::MapReduceApp;
use replayer::message;
use replayer::utils;
use replayer::Node;

fn main() -> anyhow::Result<()> {
    logging::init_log();

    let num_workers: usize = std::env::var("RP_NUM_WORKER")?.parse()?;
    log::debug!("num_workers: {}", num_workers);

    let controller_uri = std::env::var("RP_CONTROLLER_URI")?;

    log::debug!("binding to controller_uri: {}", controller_uri);
    let listener = std::net::TcpListener::bind(controller_uri)?;

    let mut workers: HashMap<Node, std::net::TcpStream> = Default::default();

    // process add node event
    while workers.len() < num_workers {
        let (mut client, addr) = listener.accept()?;
        log::debug!("accept an incoming connection from addr: {}", addr);

        let payload_len = utils::read_payload_len(&mut client)? as usize;

        let mut buf = Vec::with_capacity(payload_len);
        unsafe {
            buf.set_len(payload_len);
        }
        client.read_exact(&mut buf)?;

        let cmd: message::Command = bincode::deserialize(&buf)?;
        log::trace!("receive a command: {:?}", cmd);

        use message::Command::*;
        match cmd {
            AddNode(node) => {
                if workers.contains_key(&node) {
                    log::error!("repeated AddNode: {:?}", node);
                }
                workers.insert(node, client);
            }
            _ => {
                log::error!("received unexpected command: {:?}", cmd);
            }
        }
    }

    // broadcast nodes to all workers
    let mut nodes: Vec<Node> = workers.keys().cloned().collect();
    // an order is useful for establishing all to all connections among workers
    nodes.sort();
    let bcast_cmd = message::Command::BroadcastNodes(nodes);
    log::debug!("broadcasting nodes: {:?}", bcast_cmd);

    let nodes_buf = bincode::serialize(&bcast_cmd)?;
    let nodes_len_buf = (nodes_buf.len() as u64).to_be_bytes();
    for worker in workers.values_mut() {
        worker.write_all(&nodes_len_buf)?;
        worker.write_all(&nodes_buf)?;
    }

    // set workers to nonblocking
    log::debug!("set workers to nonblocking");
    for worker in workers.values_mut() {
        worker.set_nonblocking(true).unwrap();
    }

    let workers = workers
        .into_iter()
        .map(|(k, v)| (k, replayer::controller::Endpoint::new(v)))
        .collect();

    // emit application flows
    let mut app = MapReduceApp::new(workers);
    app.start()?;

    io_loop(app)
}

fn io_loop(mut app: MapReduceApp) -> anyhow::Result<()> {
    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(1024);

    let mut token_table: Vec<Node> = Default::default();
    for (node, ep) in app.workers_mut().iter_mut() {
        let new_token = mio::Token(token_table.len());
        token_table.push(node.clone());

        poll.registry().register(
            ep.stream_mut(),
            new_token,
            mio::Interest::READABLE | mio::Interest::WRITABLE,
        )?;
    }

    let mut handler = Handler::new(app.workers().len());

    'outer: loop {
        poll.poll(&mut events, None)?;
        for event in events.iter() {
            assert!(event.token().0 < token_table.len());
            let node = &token_table[event.token().0];
            let ep = app.workers_mut().get_mut(node).unwrap();
            if event.is_writable() {
                ep.on_send_ready()?;
            }
            if event.is_readable() {
                match ep.on_recv_ready() {
                    Ok(cmd) => {
                        if handler.handle_cmd(cmd, &mut app)? {
                            break 'outer;
                        }
                    }
                    Err(e) => {
                        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                            if io_err.kind() != std::io::ErrorKind::WouldBlock {
                                return Err(e);
                            }
                        }
                        return Err(e);
                    }
                }
            }
        }
    }

    Ok(())
}

struct Handler {
    num_remaining: usize,
}

impl Handler {
    fn new(num_workers: usize) -> Self {
        Handler {
            num_remaining: num_workers,
        }
    }

    fn handle_cmd(
        &mut self,
        cmd: message::Command,
        app: &mut MapReduceApp,
    ) -> anyhow::Result<bool> {
        use message::Command::*;
        match cmd {
            FlowComplete(ref _flow) => {
                app.on_event(cmd)?;
            }
            LeaveNode(_node) => {
                self.num_remaining -= 1;
                if self.num_remaining == 0 {
                    return Ok(true);
                }
            }
            _ => {
                log::error!("handle_msg: unexpected cmd: {:?}", cmd);
            }
        }

        Ok(false)
    }
}
