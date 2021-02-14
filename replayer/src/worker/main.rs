#![feature(option_unwrap_none)]

use std::collections::HashMap;

use replayer::endpoint::Endpoint;
use replayer::message;
use replayer::utils;
use replayer::Node;

fn get_hostname() -> String {
    use std::process::Command;
    let result = Command::new("hostname").output().unwrap();
    assert!(result.status.success());
    std::str::from_utf8(&result.stdout).unwrap().to_owned()
}

fn join_group(my_node: Node, controller: &mut std::net::TcpStream) -> anyhow::Result<()> {
    // send AddNode message to join group
    let hostname = get_hostname();
    let cmd = message::Command::AddNode(my_node, hostname);
    utils::send_cmd_sync(controller, &cmd)?;
    Ok(())
}

const BASE_PORT: u16 = 30000;
const MAX_RETRY: u16 = 100;

fn find_avail_port() -> anyhow::Result<u16> {
    let mut port = BASE_PORT;
    let mut max_retries = MAX_RETRY;

    loop {
        match std::net::TcpListener::bind(("0.0.0.0", port)) {
            Ok(_) => {
                break;
            }
            Err(e) => {
                port += 1;
                max_retries -= 1;
                if max_retries == 0 {
                    return Err(e.into());
                }
            }
        }
    }

    Ok(port)
}

fn main() -> anyhow::Result<()> {
    logging::init_log();

    log::debug!("finding available port to bind");
    let port = find_avail_port()?;

    log::debug!("binding to port: {:?}", port);
    let listener = std::net::TcpListener::bind(("0.0.0.0", port))?;

    let controller_uri = std::env::var("RP_CONTROLLER_URI").expect("RP_CONTROLLER_URI");
    log::debug!("connecting to controller_uri: {}", controller_uri);

    let mut controller =
        std::net::TcpStream::connect(controller_uri.clone()).expect(&controller_uri);

    let my_node = Node {
        addr: controller.local_addr()?.ip().to_string(),
        port,
    };

    // send AddNode message
    join_group(my_node.clone(), &mut controller)?;

    // wait for BroadcastNodes message
    let nodes = {
        let bcast_cmd = utils::recv_cmd_sync(&mut controller)?;
        use message::Command::*;
        match bcast_cmd {
            BroadcastNodes(nodes) => nodes,
            _ => panic!("unexpected cmd: {:?}", bcast_cmd),
        }
    };

    // establish connections to all peers
    // 1<-2, 1<-3, 2<-3,...

    // only address, no port included, this should match the src and dst field in struct Flow
    let mut active_peers: HashMap<Node, std::net::TcpStream> = Default::default();

    // usually n - m - 1, but remember we also accept connection from ourself, so n - m
    let num_passive = nodes.len()
        - nodes.iter().position(|n| n == &my_node).expect(&format!(
            "my_node: {:?} not found in nodes: {:?}",
            my_node, nodes
        ));
    log::debug!("number of connections to accept: {}", num_passive);

    let my_node_copy = my_node.clone();

    // start an seperate thread for accepting connections is the most easy way
    let accept_thread_handle = std::thread::spawn(
        move || -> anyhow::Result<HashMap<Node, std::net::TcpStream>> {
            let mut passive_peers: HashMap<_, _> = Default::default();

            while passive_peers.len() < num_passive {
                let (mut stream, addr) = listener.accept()?;
                log::debug!("worker accepts an incoming connection from addr: {}", addr);
                // receive AddNodePeer command
                let cmd = utils::recv_cmd_sync(&mut stream)?;
                log::trace!("receive a command: {:?}", cmd);
                match cmd {
                    message::Command::AddNodePeer(node) => {
                        if node == my_node_copy {
                            let renamed_node = Node {
                                addr: node.addr,
                                port: 0,
                            };
                            passive_peers.insert(renamed_node, stream).unwrap_none();
                        } else {
                            passive_peers.insert(node, stream).unwrap_none();
                        }
                    }
                    _ => panic!("unexpected cmd: {:?}", cmd),
                }
            }

            Ok(passive_peers)
        },
    );

    for node in &nodes {
        let mut peer = std::net::TcpStream::connect((node.addr.clone(), node.port))?;

        utils::send_cmd_sync(&mut peer, &message::Command::AddNodePeer(my_node.clone()))?;

        active_peers.insert(node.clone(), peer).unwrap_none();

        // everyone also connects to itself
        if node == &my_node {
            break;
        }
    }

    let passive_peers = accept_thread_handle.join().unwrap()?;

    // merge active and passive peers
    let peers: HashMap<Node, Endpoint> = active_peers
        .into_iter()
        .chain(passive_peers.into_iter())
        .map(|(n, stream)| {
            log::debug!("set {:?} to nonblocking", n);
            stream.set_nonblocking(true).unwrap();
            (n, Endpoint::new(stream))
        })
        .collect();
    assert_eq!(peers.len(), nodes.len() + 1);

    controller.set_nonblocking(true).unwrap();
    let controller = Endpoint::new(controller);
    io_loop(my_node, controller, peers)
}

fn io_loop(
    my_node: Node,
    mut controller: Endpoint,
    mut peers: HashMap<Node, Endpoint>,
) -> anyhow::Result<()> {
    log::debug!("entering io looping");

    let poll = mio::Poll::new()?;
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
        // only call epoll_wait after read or write return EAGAIN
        poll.poll(&mut events, None)?;
        for event in events.iter() {
            assert!(event.token().0 <= token_table.len());

            let ep = if event.token().0 == token_table.len() {
                // controller sent a command to me
                log::trace!("receive a event from controller");
                &mut controller
            } else {
                // receive data from other workers
                let node = &token_table[event.token().0];
                log::trace!("receive a event from worker {:?}", node);
                peers.get_mut(node).unwrap()
            };

            if event.readiness().is_writable() {
                println!("event is writable");
            } else if event.readiness().is_readable() {
                println!("event is readable");
            }

            if event.readiness().is_writable() {
                ep.on_send_ready()?;
            }
            if event.readiness().is_readable() {
                match ep.on_recv_ready() {
                    Ok(cmd) => {
                        if handler.handle_cmd(cmd, &mut controller, &mut peers)? {
                            break 'outer;
                        }
                    }
                    Err(e) => {
                        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                            if io_err.kind() != std::io::ErrorKind::WouldBlock {
                                return Err(e);
                            }
                        } else {
                            return Err(e);
                        }
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
