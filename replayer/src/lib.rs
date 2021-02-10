use nethint::Token;
use serde::{Deserialize, Serialize};

pub mod buffer;
pub mod message;

pub mod controller;
pub mod mapreduce;

pub mod worker;

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Node {
    pub addr: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Flow {
    pub bytes: usize,
    // src, dst are only resolvable addresses, no port number included
    pub src: String,
    pub dst: String,
    pub token: Option<Token>,
}

impl Flow {
    pub fn new(bytes: usize, src: &str, dst: &str, token: Option<Token>) -> Self {
        Flow {
            bytes,
            src: src.to_owned(),
            dst: dst.to_owned(),
            token,
        }
    }
}

pub mod utils {
    use std::convert::TryInto;
    use std::io::Read;

    pub fn read_be_u64(input: &[u8]) -> u64 {
        // we know the length of input passed in is 8, so just unwrap it
        u64::from_be_bytes(input.try_into().unwrap())
    }

    pub fn read_payload_len(stream: &mut std::net::TcpStream) -> anyhow::Result<u64> {
        let mut buf = Vec::with_capacity(std::mem::size_of::<u64>());
        unsafe {
            buf.set_len(std::mem::size_of::<u64>());
        }
        stream.read_exact(&mut buf)?;
        Ok(read_be_u64(&buf))
    }
}
