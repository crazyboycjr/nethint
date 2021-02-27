use crate::{Node, Role};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use litemsg::endpoint;

pub struct Communicator {
    my_rank: usize,
    nodes: Vec<Node>,
    peers: Vec<endpoint::Endpoint>,

    controller: TcpStream,
    workers: Option<HashMap<Node, TcpStream>>,
}

impl Communicator {
    pub fn new(my_role: Role) -> anyhow::Result<Self> {
        let controller_uri = std::env::var("NH_CONTROLLER_URI").expect("NH_CONTROLLER_URI");

        let workers = if my_role == Role::GlobalLeader {
            // start controller
            let num_workers = std::env::var("NH_NUM_WORKER")
                .expect("NH_NUM_WORKER")
                .parse()
                .expect("NH_NUM_WORKER");
            Some(litemsg::accept_peers(&controller_uri, num_workers)?)
        } else {
            None
        };

        let (nodes, my_node, controller, mut listener) = litemsg::connect_controller(&controller_uri)?;
        let my_rank = nodes.iter().position(|n| n == &my_node).unwrap();

        let peers = litemsg::connect_peers2(&nodes, &my_node, &mut listener)?;

        let peers = peers.into_iter().map(|b| {
            b.readable(true).writable(true).build().unwrap()
        }).collect();

        Ok(Communicator {
            my_rank,
            nodes,
            peers,
            controller,
            workers,
        })
    }

    pub fn world_size(&self) -> usize {
        self.nodes.len()
    }
}
