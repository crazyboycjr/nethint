use nethint::Token;
use serde::{Deserialize, Serialize};

pub mod buffer;
pub mod message;
pub mod endpoint;

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

pub mod utils {
    use crate::message;
    use std::convert::TryInto;
    use std::io::{Read, Write};

    pub fn read_be_u64(input: &[u8]) -> u64 {
        // we know the length of input passed in is 8, so just unwrap it
        u64::from_be_bytes(input.try_into().unwrap())
    }

    // These APIs below require a socket in blocking state

    pub fn read_payload_len(stream: &mut std::net::TcpStream) -> anyhow::Result<u64> {
        let mut buf = Vec::with_capacity(std::mem::size_of::<u64>());
        unsafe {
            buf.set_len(std::mem::size_of::<u64>());
        }
        stream.read_exact(&mut buf)?;
        Ok(read_be_u64(&buf))
    }

    pub fn recv_message_sync(stream: &mut std::net::TcpStream) -> anyhow::Result<Vec<u8>> {
        let payload_len = read_payload_len(stream)? as usize;
        let mut buf = Vec::with_capacity(payload_len);
        unsafe {
            buf.set_len(payload_len);
        }

        stream.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn send_message_sync(stream: &mut std::net::TcpStream, buf: &[u8]) -> anyhow::Result<()> {
        let len_buf = (buf.len() as u64).to_be_bytes();
        stream.write_all(&len_buf)?;
        stream.write_all(&buf)?;
        Ok(())
    }

    pub fn recv_cmd_sync(stream: &mut std::net::TcpStream) -> anyhow::Result<message::Command> {
        let buf = recv_message_sync(stream)?;
        let cmd = bincode::deserialize(&buf)?;
        Ok(cmd)
    }

    pub fn send_cmd_sync(
        stream: &mut std::net::TcpStream,
        cmd: &message::Command,
    ) -> anyhow::Result<()> {
        let buf = bincode::serialize(cmd)?;
        send_message_sync(stream, &buf)
    }
}
