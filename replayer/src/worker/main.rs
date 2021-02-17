#![feature(option_unwrap_none)]

use std::collections::HashMap;
use std::net::TcpStream;

use replayer::message;
use replayer::Node;

use litemsg::endpoint::{self, Endpoint};

fn main() -> anyhow::Result<()> {
    logging::init_log();

    let controller_uri = std::env::var("RP_CONTROLLER_URI").expect("RP_CONTROLLER_URI");
    log::info!("connecting to controller_uri: {}", controller_uri);

    let (nodes, my_node, controller, listener) = litemsg::connect_controller(&controller_uri)?;

    let peers = litemsg::connect_peers(&nodes, &my_node, listener)?;

    io_loop(my_node, controller, peers)
}

fn io_loop(
    my_node: Node,
    controller: TcpStream,
    peers: HashMap<Node, TcpStream>,
) -> anyhow::Result<()> {
    log::info!("entering io looping");

    let poll = mio::Poll::new()?;

    let mut controller = Endpoint::new(controller, &poll);
    let mut peers: HashMap<Node, Endpoint> = peers
        .into_iter()
        .map(|(n, stream)| (n, Endpoint::new(stream, &poll)))
        .collect();

    let mut events = mio::Events::with_capacity(1024);

    let mut token_table: Vec<Node> = Default::default();
    for (node, ep) in peers.iter_mut() {
        let new_token = mio::Token(token_table.len());
        token_table.push(node.clone());

        poll.register(
            ep.stream(),
            new_token,
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::level(),
        )?;
    }

    poll.register(
        controller.stream(),
        mio::Token(token_table.len()),
        mio::Ready::readable() | mio::Ready::writable(),
        mio::PollOpt::level(),
    )?;

    let mut handler = Handler::new();

    'outer: loop {
        poll.poll(&mut events, None)?;
        for event in events.iter() {
            assert!(event.token().0 <= token_table.len());

            let ep = if event.token().0 == token_table.len() {
                // controller sent a command to me
                &mut controller
            } else {
                // receive data from other workers
                let node = &token_table[event.token().0];
                peers.get_mut(node).unwrap()
            };

            if event.readiness().is_writable() {
                match ep.on_send_ready() {
                    Ok(_) => {}
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
            if event.readiness().is_readable() {
                match ep.on_recv_ready() {
                    Ok(cmd) => {
                        if handler.handle_cmd(cmd, &mut controller, &mut peers)? {
                            break 'outer;
                        }
                    }
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(endpoint::Error::ConnectionLost) => {
                        let node = &token_table[event.token().0];
                        peers.remove(&node).unwrap();
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    // send LeaveNode
    // controller.stream_mut().set_nonblocking(false);
    controller.post(message::Command::LeaveNode(my_node))?;
    loop {
        match controller.on_send_ready() {
            Ok(()) => break,
            Err(endpoint::Error::WouldBlock) => {}
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    Ok(())
}

struct Handler {}

impl Handler {
    fn new() -> Self {
        Handler {}
    }

    fn handle_cmd(
        &mut self,
        cmd: message::Command,
        controller: &mut Endpoint,
        peers: &mut HashMap<Node, Endpoint>,
    ) -> anyhow::Result<bool> {
        use message::Command::*;
        match cmd {
            EmitFlow(flow) => {
                let dst_ep = peers.get_mut(&flow.dst).unwrap();
                let mut data = Vec::with_capacity(flow.bytes);
                unsafe {
                    data.set_len(flow.bytes);
                }
                let msg = Data(flow, data);
                dst_ep.post(msg)?;
            }
            AppFinish => {
                return Ok(true);
            }
            Data(flow, _) => {
                // flow received, notify controller with FlowComplete
                let msg = FlowComplete(flow);
                controller.post(msg)?;
            }
            _ => {
                log::error!("handle_msg: unexpected cmd: {:?}", cmd);
            }
        }

        Ok(false)
    }
}
