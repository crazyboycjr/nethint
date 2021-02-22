#![feature(str_split_once)]
#![feature(option_unwrap_none)]

pub mod cluster;
pub mod sampler;
pub mod message;
pub mod communicator;

pub use litemsg::Node;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    GlobalLeader,
    RackLeader,
    Worker,
}
