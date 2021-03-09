use nethint::Token;
use serde::{Deserialize, Serialize};

pub mod message;

pub mod controller;

pub mod worker;

pub use litemsg::Node;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Flow {
    pub bytes: usize,
    pub src: Node,
    pub dst: Node,
    pub token: Option<Token>,
}

impl Flow {
    pub fn new(bytes: usize, src: Node, dst: Node, token: Option<Token>) -> Self {
        Flow {
            bytes,
            src,
            dst,
            token,
        }
    }
}
