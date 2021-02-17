#![feature(option_unwrap_none)]
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};

pub mod buffer;
pub mod command;
pub mod endpoint;
pub mod utils;

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Node {
    pub addr: String,
    pub port: u16,
}

pub fn accept_peers(
    controller_uri: &str,
    num_workers: usize,
) -> anyhow::Result<HashMap<Node, TcpStream>> {
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

        use command::Command::*;
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
    let bcast_cmd = command::Command::BroadcastNodes(nodes);
    log::debug!("broadcasting nodes: {:?}", bcast_cmd);

    for worker in workers.values_mut() {
        utils::send_cmd_sync(worker, &bcast_cmd).unwrap();
    }

    Ok(workers)
}

// nodes, my_node, controller, listener
pub fn connect_controller(
    controller_uri: &str,
) -> anyhow::Result<(Vec<Node>, Node, TcpStream, TcpListener)> {
    log::info!("finding available port to bind");
    let port = utils::find_avail_port()?;

    log::info!("binding to port: {:?}", port);
    let listener = std::net::TcpListener::bind(("0.0.0.0", port))?;

    let mut controller = utils::connect_retry(&controller_uri, 5)?;

    let my_node = Node {
        addr: controller.local_addr()?.ip().to_string(),
        port,
    };

    // send AddNode message
    utils::add_node(my_node.clone(), &mut controller)?;

    // wait for BroadcastNodes message
    let bcast_cmd = utils::recv_cmd_sync(&mut controller)?;
    use command::Command::*;
    match bcast_cmd {
        BroadcastNodes(nodes) => Ok((nodes, my_node, controller, listener)),
        _ => panic!("unexpected cmd: {:?}", bcast_cmd),
    }
}

// peers
pub fn connect_peers(
    nodes: &[Node],
    my_node: &Node,
    listener: TcpListener,
) -> anyhow::Result<HashMap<Node, TcpStream>> {
    // establish connections to all peers
    // 1<-2, 1<-3, 2<-3,...

    // only address, no port included, this should match the src and dst field in struct Flow
    let mut active_peers: HashMap<Node, TcpStream> = Default::default();

    // usually n - m - 1, but remember we also accept connection from ourself, so n - m
    let num_passive = nodes.len()
        - nodes
            .iter()
            .position(|n| n == my_node)
            .unwrap_or_else(|| panic!("my_node: {:?} not found in nodes: {:?}", my_node, nodes));
    log::debug!("number of connections to accept: {}", num_passive);

    let my_node_copy = my_node.clone();

    // start an seperate thread for accepting connections is the most easy way
    let accept_thread_handle =
        std::thread::spawn(move || -> anyhow::Result<HashMap<Node, TcpStream>> {
            let mut passive_peers: HashMap<_, _> = Default::default();

            while passive_peers.len() < num_passive {
                let (mut stream, addr) = listener.accept()?;
                log::debug!("worker accepts an incoming connection from addr: {}", addr);

                // receive AddNodePeer command
                let cmd = utils::recv_cmd_sync(&mut stream)?;
                log::trace!("receive a command: {:?}", cmd);
                match cmd {
                    command::Command::AddNodePeer(node) => {
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
        });

    for node in nodes {
        let mut peer = TcpStream::connect((node.addr.clone(), node.port))?;

        utils::send_cmd_sync(&mut peer, &command::Command::AddNodePeer(my_node.clone()))?;

        active_peers.insert(node.clone(), peer).unwrap_none();

        // everyone also connects to itself
        if node == my_node {
            break;
        }
    }

    let passive_peers = accept_thread_handle.join().unwrap()?;

    // merge active and passive peers
    let peers: HashMap<Node, TcpStream> = active_peers
        .into_iter()
        .chain(passive_peers.into_iter())
        .collect();
    assert_eq!(peers.len(), nodes.len() + 1);

    Ok(peers)
}
