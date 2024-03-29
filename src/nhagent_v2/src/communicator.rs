use crate::message;
use crate::{Node, Role};
use litemsg::endpoint;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::rc::Rc;

pub type BcastId = u64;

pub struct Communicator {
    my_rank: usize,
    my_role: Role,
    nodes: Vec<Node>,
    peers: Vec<endpoint::Endpoint>,

    _listener: TcpListener,
    controller: TcpStream,
    // only rack leader has these fields set to Some
    workers: Option<HashMap<Node, TcpStream>>,
    controller_listener: Option<mio::net::TcpListener>,

    // only for controller/global leader
    apps: Vec<endpoint::Endpoint>,

    poll: Option<Rc<mio::Poll>>,

    // broadcast state, bcast_id -> (received, total)
    bcast: HashMap<BcastId, (usize, usize)>,
    bcast_id: BcastId,
}

impl Communicator {
    pub fn new(my_role: Role) -> anyhow::Result<Self> {
        let controller_uri = std::env::var("NH_CONTROLLER_URI").expect("NH_CONTROLLER_URI");

        let controller_uri2 = controller_uri.clone();
        let handle = std::thread::spawn(move || {
            if my_role == Role::GlobalLeader {
                // start controller
                let num_agents = std::env::var("NH_NUM_RACKS")
                    .expect("NH_NUM_RACKS")
                    .parse()
                    .expect("NH_NUM_RACKS");
                Some(litemsg::accept_peers(&controller_uri2, num_agents).unwrap())
            } else {
                None
            }
        });

        let (nodes, my_node, controller, mut listener) =
            litemsg::connect_controller(&controller_uri, 10)?;
        log::debug!("connected to controller");
        log::info!("nodes: {:?}", nodes);
        let my_rank = nodes.iter().position(|n| n == &my_node).unwrap();

        let peers = litemsg::connect_peers2(&nodes, &my_node, &mut listener)?;
        log::debug!("connected to peers");

        let peers = peers
            .into_iter()
            .map(|b| b.readable(true).writable(true).build().unwrap())
            .collect();

        let ret = handle.join().unwrap();
        let (controller_listener, workers) = if ret.is_some() {
            let t = ret.unwrap();
            let listener = mio::net::TcpListener::from_std(t.0)?;
            (Some(listener), Some(t.1))
        } else {
            (None, None)
        };
        Ok(Communicator {
            my_rank,
            my_role,
            nodes,
            peers,
            _listener: listener,
            controller,
            workers,
            controller_listener,
            apps: Vec::new(),
            poll: None,
            bcast: HashMap::new(),
            bcast_id: 0,
        })
    }

    pub fn set_poll(&mut self, poll: Rc<mio::Poll>) {
        self.poll = Some(poll);
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

    pub fn controller_listener(&self) -> Option<&mio::net::TcpListener> {
        self.controller_listener.as_ref()
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

    pub fn app_ep(&self, rank: usize) -> &endpoint::Endpoint {
        assert_eq!(self.my_role, Role::GlobalLeader);
        assert!(rank > self.world_size(), "rank: {}", rank);
        &self.apps[rank - self.world_size() - 1]
    }

    pub fn app_ep_mut(&mut self, rank: usize) -> &mut endpoint::Endpoint {
        assert_eq!(self.my_role, Role::GlobalLeader);
        assert!(rank > self.world_size(), "rank: {}", rank);
        let pos = rank - self.world_size() - 1;
        &mut self.apps[pos]
    }

    pub fn send_to(&mut self, rank: usize, msg: &message::Message) -> anyhow::Result<()> {
        assert!(rank != self.world_size());
        let ep = {
            let ep = if rank < self.world_size() {
                self.peer_mut(rank)
            } else {
                self.app_ep_mut(rank)
            };
            ep.post(msg, None)?;

            &*ep
        };

        // reactivate
        let s = unsafe { &*(ep.stream() as *const mio::net::TcpStream) };
        let interest = ep.interest();
        self.poll.as_ref().unwrap().reregister(
            s,
            mio::Token(rank),
            interest,
            mio::PollOpt::level(),
        )?;

        Ok(())
    }

    pub fn broadcast(&mut self, msg: message::Message) -> anyhow::Result<()> {
        let bcast_msg = message::Message::BcastMessage(self.bcast_id, Box::new(msg));
        for i in 0..self.world_size() {
            if i == self.my_rank() {
                continue;
            }
            let ep = self.peer_mut(i);
            ep.post(&bcast_msg, None)?;
        }
        self.bcast.insert(self.bcast_id, (0, self.world_size() - 1));
        self.bcast_id += 1;
        Ok(())
    }

    pub fn recv_bcast_msg(&mut self, bcast_id: BcastId) {
        use std::collections::hash_map::Entry;
        match self.bcast.entry(bcast_id) {
            Entry::Occupied(mut e) => {
                e.get_mut().0 += 1;
                if e.get().0 == e.get().1 {
                    e.remove_entry();
                }
            }
            Entry::Vacant(_) => {
                panic!("invalid bcast_id: {}", bcast_id);
            }
        }
    }

    pub fn bcast_done(&self) -> bool {
        self.bcast.is_empty()
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

    pub fn accept_app_connection(&mut self) -> anyhow::Result<()> {
        assert_eq!(self.my_role, Role::GlobalLeader);
        let (client, addr) = self.controller_listener().unwrap().accept_std()?;

        log::debug!(
            "controller accepts an incoming connection from app addr: {}",
            addr
        );

        let builder = endpoint::Builder::new()
            .stream(client)
            .readable(true)
            .writable(true)
            .node(addr.to_string().parse().unwrap());
        let ep = builder.build().unwrap();
        let token = mio::Token(self.world_size() + self.apps.len() + 1);
        self.poll.as_ref().unwrap().register(
            ep.stream(),
            token,
            ep.interest(),
            mio::PollOpt::level(),
        )?;
        self.apps.push(ep);
        Ok(())
    }
}
