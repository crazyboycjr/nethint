#![feature(str_split_once)]
#![feature(option_unwrap_none)]
#![feature(command_access)]

pub mod cluster;
pub mod sampler;
pub mod message;
pub mod communicator;
pub mod utils;

pub use litemsg::Node;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    GlobalLeader,
    RackLeader,
    Worker,
}
