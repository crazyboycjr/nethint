use std::collections::HashMap;

use replayer::mapreduce::MapReduceApp;
use replayer::message;
use replayer::utils;
use replayer::Node;

fn main() -> anyhow::Result<()> {
    logging::init_log();

    let num_workers: usize = std::env::var("RP_NUM_WORKER")
        .expect("RP_NUM_WORKER")
        .parse()
        .expect("RP_NUM_WORKER");
    log::debug!("num_workers: {}", num_workers);

    let controller_uri = std::env::var("RP_CONTROLLER_URI").expect("RP_CONTROLLER_URI");

    log::debug!("binding to controller_uri: {}", controller_uri);
    let listener = std::net::TcpListener::bind(controller_uri.clone()).expect(&controller_uri);

    let mut workers: HashMap<Node, std::net::TcpStream> = Default::default();

    // process add node event
    while workers.len() < num_workers {
        let (mut client, addr) = listener.accept()?;
        log::debug!(
            "controller accepts an incoming connection from addr: {}",
            addr
        );

        let cmd = utils::recv_cmd_sync(&mut client)?;
        log::trace!("receive a command: {:?}", cmd);

        use message::Command::*;
        match cmd {
            AddNode(node, _hostname) => {
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

    for worker in workers.values_mut() {
        utils::send_cmd_sync(worker, &bcast_cmd).unwrap();
    }

    io_loop(workers)
}

fn io_loop(workers: HashMap<Node, std::net::TcpStream>) -> anyhow::Result<()> {
    let poll = mio::Poll::new()?;

    let workers = workers
        .into_iter()
        .map(|(k, stream)| (k, replayer::endpoint::Endpoint::new(stream, &poll)))
        .collect();

    // emit application flows
    let mut app = MapReduceApp::new(workers);
    app.start()?;

    let mut events = mio::Events::with_capacity(1024);

    let mut token_table: Vec<Node> = Default::default();
    for (node, ep) in app.workers_mut().iter_mut() {
        let new_token = mio::Token(token_table.len());
        token_table.push(node.clone());

        poll.register(
            ep.stream_mut(),
            new_token,
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::level(),
        )?;
    }

    let mut handler = Handler::new(app.workers().len());

    'outer: loop {
        poll.poll(&mut events, None)?;
        for event in events.iter() {
            assert!(event.token().0 < token_table.len());
            let node = &token_table[event.token().0];
            let ep = app.workers_mut().get_mut(node).unwrap();
            if event.readiness().is_writable() {
                match ep.on_send_ready() {
                    Ok(_) => {}
                    Err(replayer::endpoint::Error::WouldBlock) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            if event.readiness().is_readable() {
                match ep.on_recv_ready() {
                    Ok(cmd) => {
                        if handler.handle_cmd(cmd, &mut app)? {
                            break 'outer;
                        }
                    }
                    Err(replayer::endpoint::Error::WouldBlock) => {}
                    Err(replayer::endpoint::Error::ConnectionLost) => {
                        // hopefully this will also call Drop for the ep
                        app.workers_mut().remove(&node).unwrap();
                    }
                    Err(e) => return Err(e.into()),
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
            LeaveNode(node) => {
                app.workers_mut().remove(&node).unwrap();
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
