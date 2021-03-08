use crate::message;
use crate::{Node, Role};
use litemsg::endpoint;
use std::collections::HashMap;
use std::net::TcpStream;

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

        let controller_uri2 = controller_uri.clone();
        let handle = std::thread::spawn(move || {
            if my_role == Role::GlobalLeader {
                // start controller
                let num_workers = std::env::var("NH_NUM_WORKER")
                    .expect("NH_NUM_WORKER")
                    .parse()
                    .expect("NH_NUM_WORKER");
                Some(litemsg::accept_peers(&controller_uri2, num_workers).unwrap())
            } else {
                None
            }
        });

        let (nodes, my_node, controller, mut listener) =
            litemsg::connect_controller(&controller_uri)?;
        log::debug!("connected to controller");
        let my_rank = nodes.iter().position(|n| n == &my_node).unwrap();

        let peers = litemsg::connect_peers2(&nodes, &my_node, &mut listener)?;
        log::debug!("connected to peers");

        let peers = peers
            .into_iter()
            .map(|b| b.readable(true).writable(true).build().unwrap())
            .collect();

        let workers = handle.join().unwrap();
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

    pub fn my_role(&self) -> Role {
        self.my_role
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
            for i in 0..self.world_size() {
                if i == self.my_rank {
                    continue;
                }
                let peer_node = &self.nodes[i];
                let msg: message::Message = litemsg::utils::recv_cmd_sync(
                    self.workers.as_mut().unwrap().get_mut(peer_node).unwrap(),
                )?;
                match msg {
                    SyncRequest(b) => assert_eq!(b, barrier_id),
                    _ => panic!("msg: {:?}", msg),
                }
            }
            for i in 0..self.world_size() {
                if i == self.my_rank {
                    continue;
                }
                let peer_node = &self.nodes[i];
                litemsg::utils::send_cmd_sync(
                    self.workers.as_mut().unwrap().get_mut(peer_node).unwrap(),
                    &SyncResponse(barrier_id),
                )?;
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
