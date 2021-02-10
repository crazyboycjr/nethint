#![feature(option_unwrap_none)]

use std::collections::HashMap;
use std::io::{Read, Write};

use replayer::message;
use replayer::utils;
use replayer::worker::Endpoint;
use replayer::Node;

fn join_group(my_node: Node, controller: &mut std::net::TcpStream) -> anyhow::Result<()> {
    // send AddNode message to join group
    let cmd = message::Command::AddNode(my_node);
    let buf = bincode::serialize(&cmd)?;
    let buf_len = u64::to_be_bytes(buf.len() as _);
    controller.write_all(&buf_len)?;
    controller.write_all(&buf)?;
    Ok(())
}

const BASE_PORT: u16 = 30000;
const MAX_RETRY: u16 = 100;

fn find_avail_port() -> anyhow::Result<Node> {
    let mut port = BASE_PORT;
    let mut max_retries = MAX_RETRY;

    loop {
        match std::net::TcpListener::bind(format!("0.0.0.0:{}", port)) {
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

    Ok(Node {
        addr: "0.0.0.0".to_string(),
        port,
    })
}

fn main() -> anyhow::Result<()> {
    logging::init_log();

    log::debug!("finding available port to bind");
    let my_node = find_avail_port()?;

    log::debug!("binding to my_node: {:?}", my_node);
    let listener = std::net::TcpListener::bind((my_node.addr.clone(), my_node.port))?;

    let controller_uri = std::env::var("RP_CONTROLLER_URI")?;
    log::debug!("connecting to controller_uri: {}", controller_uri);

    let mut controller = std::net::TcpStream::connect(controller_uri)?;

    // send AddNode message
    join_group(my_node.clone(), &mut controller)?;

    // wait for BroadcastNodes message
    let nodes = {
        let payload_len = utils::read_payload_len(&mut controller)? as usize;
        let mut buf = Vec::with_capacity(payload_len);
        unsafe {
            buf.set_len(payload_len);
        }

        controller.read_exact(&mut buf)?;
        let bcast_cmd: message::Command = bincode::deserialize(&buf)?;
        use message::Command::*;
        match bcast_cmd {
            BroadcastNodes(nodes) => nodes,
            _ => {
                panic!("unexpected cmd: {:?}", bcast_cmd);
            }
        }
    };

    // establish connections to all peers
    // 1<-2, 1<-3, 2<-3,...

    // only address, no port included, this should match the src and dst field in struct Flow
    let mut active_peers: HashMap<String, std::net::TcpStream> = Default::default();

    // usually n - m - 1, but remember we also accept connection from ourself, so n - m
    let num_passive = nodes.len()
        - nodes.iter().position(|n| n == &my_node).expect(&format!(
            "my_node: {:?} not found in nodes: {:?}",
            my_node, nodes
        ));

    // start an seperate thread for accepting connections is the most easy way
    let my_node_copy = my_node.clone();
    let accept_thread_handle = std::thread::spawn(
        move || -> anyhow::Result<HashMap<String, std::net::TcpStream>> {
            let mut passive_peers: HashMap<String, std::net::TcpStream> = Default::default();
            while passive_peers.len() < num_passive {
                let (stream, addr) = listener.accept()?;
                // do not insert if it is a loopback connection, but only accept it
                let ip = addr.ip().to_string();
                if ip != my_node_copy.addr {
                    passive_peers.insert(ip, stream).unwrap_none();
                }
            }
            anyhow::Result::Ok(passive_peers)
        },
    );

    for node in &nodes {
        let peer = std::net::TcpStream::connect((node.addr.clone(), node.port))?;
        active_peers.insert(node.addr.clone(), peer).unwrap_none();

        // everyone also connects to itself
        if node == &my_node {
            break;
        }
    }

    let passive_peers = accept_thread_handle.join().unwrap()?;

    // merge active and passive peers
    let peers: HashMap<String, Endpoint> = active_peers
        .into_iter()
        .chain(passive_peers.into_iter())
        .map(|(n, stream)| (n, Endpoint::new(stream)))
        .collect();
    assert_eq!(peers.len(), nodes.len());

    let controller = Endpoint::new(controller);
    io_loop(my_node, controller, peers)
}

fn io_loop(
    my_node: Node,
    mut controller: Endpoint,
    mut peers: HashMap<String, Endpoint>,
) -> anyhow::Result<()> {
    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(1024);

    let mut token_table: Vec<String> = Default::default();
    for (addr, ep) in peers.iter_mut() {
        let new_token = mio::Token(token_table.len());
        token_table.push(addr.clone());

        poll.registry().register(
            ep.stream_mut(),
            new_token,
            mio::Interest::READABLE | mio::Interest::WRITABLE,
        )?;
    }

    poll.registry().register(
        controller.stream_mut(),
        mio::Token(token_table.len()),
        mio::Interest::READABLE | mio::Interest::WRITABLE,
    )?;

    let mut handler = Handler::new();

    'outer: loop {
        poll.poll(&mut events, None)?;
        for event in events.iter() {
            assert!(event.token().0 <= token_table.len());

            let ep = if event.token().0 == token_table.len() {
                // controller sent a command to us
                &mut controller
            } else {
                // receive data from other workers
                let ip = &token_table[event.token().0];
                peers.get_mut(ip).unwrap()
            };

            if event.is_writable() {
                ep.on_send_ready()?;
            }
            if event.is_readable() {
                match ep.on_recv_ready() {
                    Ok(cmd) => {
                        if handler.handle_cmd(cmd, &mut peers)? {
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
        peers: &mut HashMap<String, Endpoint>,
    ) -> anyhow::Result<bool> {
        use message::Command::*;
        match cmd {
            EmitFlow(flow) => {
                let dst_ep = peers.get_mut(&flow.dst).unwrap();
                let mut data = Vec::with_capacity(flow.bytes);
                unsafe {
                    data.set_len(flow.bytes);
                }
                let msg = Data(data);
                dst_ep.post(msg)?;
            }
            AppFinish => {
                return Ok(true);
            }
            Data(_) => {
                // TODO(cjr): add some computation
            }
            _ => {
                log::error!("handle_msg: unexpected cmd: {:?}", cmd);
            }
        }

        Ok(false)
    }
}
