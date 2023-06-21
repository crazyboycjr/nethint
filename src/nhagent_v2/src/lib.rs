pub mod cluster;
pub mod sampler;
pub mod message;
pub mod communicator;
pub mod argument;
pub mod sdn_controller;

pub use litemsg::Node;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    GlobalLeader,
    RackLeader,
}
