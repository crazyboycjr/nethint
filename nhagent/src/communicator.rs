use crate::{Node, Role};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use litemsg::endpoint;
use crate::message;

pub struct Communicator {
    my_rank: usize,
    my_role: Role,
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
            my_role,
            nodes,
            peers,
            controller,
            workers,
        })
    }

    pub fn world_size(&self) -> usize {
        self.nodes.len()
    }

    pub fn my_rank(&self) -> usize {
        self.my_rank
    }

    pub fn peer(&self, rank: usize) -> &endpoint::Endpoint {
        assert!(rank != self.my_rank, "rank: {}", rank);
        if rank < self.my_rank {
            &self.peers[rank]
        } else {
            &self.peers[rank - 1]
        }
    }

    pub fn peer_mut(&mut self, rank: usize) -> &mut endpoint::Endpoint {
        assert!(rank != self.my_rank, "rank: {}", rank);
        if rank < self.my_rank {
            &mut self.peers[rank]
        } else {
            &mut self.peers[rank - 1]
        }
    }

    pub fn send_to(&mut self, rank: usize, msg: &message::Message) -> anyhow::Result<()> {
        let ep = self.peer_mut(rank);
        ep.post(msg, None)
    }

    pub fn broadcast(&mut self, msg: &message::Message) -> anyhow::Result<()> {
        for i in 0..self.world_size() {
            if i == self.my_rank() {
                continue;
            }
            let ep = self.peer_mut(i);
            ep.post(msg, None)?;
        }
        Ok(())
    }

    pub fn barrier(&mut self, barrier_id: u64) -> anyhow::Result<()> {
        use message::Message::*;

        if self.my_role == Role::GlobalLeader {
            for _i in 0..self.world_size() {
                let msg: message::Message = litemsg::utils::recv_cmd_sync(&mut self.controller)?;
                match msg {
                    SyncRequest(b) => assert_eq!(b, barrier_id),
                    _ => panic!("msg: {:?}", msg),
                }
            }
            for _i in 0..self.world_size() {
                litemsg::utils::send_cmd_sync(&mut self.controller, &SyncRequest(barrier_id))?;
            }
        } else {
            litemsg::utils::send_cmd_sync(&mut self.controller, &SyncRequest(barrier_id))?;
            let msg = litemsg::utils::recv_cmd_sync(&mut self.controller)?;
            match msg {
                SyncResponse(b) => assert_eq!(b, barrier_id),
                _ => panic!("msg: {:?}", msg),
            }
        }

        Ok(())
    }
}
