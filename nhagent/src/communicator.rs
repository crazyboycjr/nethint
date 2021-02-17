use crate::{Node, Role};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};

pub struct Communicator {
    my_rank: usize,
    peers: HashMap<usize, TcpStream>,
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

        let (nodes, my_node, controller, listener) = litemsg::connect_controller(&controller_uri)?;
        let my_rank = nodes.iter().position(|n| n == &my_node).unwrap();

        let mut peers = litemsg::connect_peers(&nodes, &my_node, listener)?;
        let peers = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| {
                let stream = peers.remove(n).unwrap();
                (i, stream)
            })
            .collect();

        Ok(Communicator {
            my_rank,
            peers,
            controller,
            workers,
        })
    }
}
