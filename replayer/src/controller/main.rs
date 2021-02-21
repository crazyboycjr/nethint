use std::collections::HashMap;

use litemsg::endpoint;
use replayer::mapreduce::MapReduceApp;
use replayer::message;
use replayer::Node;

fn main() -> anyhow::Result<()> {
    logging::init_log();

    let num_workers: usize = std::env::var("RP_NUM_WORKER")
        .expect("RP_NUM_WORKER")
        .parse()
        .expect("RP_NUM_WORKER");
    log::debug!("num_workers: {}", num_workers);

    let controller_uri = std::env::var("RP_CONTROLLER_URI").expect("RP_CONTROLLER_URI");

    let workers = litemsg::accept_peers(&controller_uri, num_workers)?;

    let start = std::time::Instant::now();
    io_loop(workers)?;
    let end = std::time::Instant::now();
    println!("duration: {:?}", end - start);

    Ok(())
}

fn io_loop(workers: HashMap<Node, std::net::TcpStream>) -> anyhow::Result<()> {
    let poll = mio::Poll::new()?;

    let workers = workers
        .into_iter()
        .map(|(k, stream)| {
            (
                k.clone(),
                endpoint::Builder::new()
                    .stream(stream)
                    .readable(true)
                    .writable(true)
                    .node(k)
                    .build()
                    .unwrap(),
            )
        })
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
                    Err(endpoint::Error::NothingToSend) => {}
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            if event.readiness().is_readable() {
                match ep.on_recv_ready() {
                    Ok((cmd, _)) => {
                        if handler.on_recv_complete(cmd, &mut app)? {
                            break 'outer;
                        }
                    }
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(endpoint::Error::ConnectionLost) => {
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

    fn on_recv_complete(
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
