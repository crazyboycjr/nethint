#![feature(str_split_once)]
#![feature(command_access)]
#![feature(map_into_keys_values)]

pub mod cluster;
pub mod sampler;
pub mod message;
pub mod communicator;
pub mod argument;
pub mod timing;

pub use litemsg::Node;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    GlobalLeader,
    RackLeader,
    Worker,
}
