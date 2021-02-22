use crate::{command, Node};
use serde::{Serialize, de::DeserializeOwned};
use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::TcpStream;

pub fn read_be_u64(input: &[u8]) -> u64 {
    // we know the length of input passed in is 8, so just unwrap it
    u64::from_be_bytes(input.try_into().unwrap())
}

// These APIs below require a socket in blocking state

pub fn read_payload_len(stream: &mut TcpStream) -> anyhow::Result<u64> {
    let mut buf = [0u8; 8];
    stream.read_exact(&mut buf)?;
    Ok(read_be_u64(&buf))
}

pub fn recv_message_sync(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let payload_len = read_payload_len(stream)? as usize;
    let mut buf = Vec::with_capacity(payload_len);
    unsafe {
        buf.set_len(payload_len);
    }

    stream.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn send_message_sync(stream: &mut TcpStream, buf: &[u8]) -> anyhow::Result<()> {
    let len_buf = (buf.len() as u64).to_be_bytes();
    stream.write_all(&len_buf)?;
    stream.write_all(&buf)?;
    Ok(())
}

pub fn recv_cmd_sync<T: DeserializeOwned>(stream: &mut TcpStream) -> anyhow::Result<T> {
    let buf = recv_message_sync(stream)?;
    let cmd = bincode::deserialize(&buf)?;
    Ok(cmd)
}

pub fn send_cmd_sync(stream: &mut TcpStream, cmd: &impl Serialize) -> anyhow::Result<()> {
    let buf = bincode::serialize(cmd)?;
    send_message_sync(stream, &buf)
}

fn get_hostname() -> String {
    use std::process::Command;
    let result = Command::new("hostname").output().unwrap();
    assert!(result.status.success());
    std::str::from_utf8(&result.stdout).unwrap().trim().to_owned()
}

pub fn add_node(my_node: Node, controller: &mut TcpStream) -> anyhow::Result<()> {
    // send AddNode message to join group
    let hostname = get_hostname();
    let cmd = command::Command::AddNode(my_node, hostname);
    send_cmd_sync(controller, &cmd)?;
    Ok(())
}

const BASE_PORT: u16 = 30000;
const MAX_RETRY: u16 = 100;

pub fn find_avail_port() -> anyhow::Result<u16> {
    let mut port = BASE_PORT;
    let mut max_retries = MAX_RETRY;

    loop {
        match std::net::TcpListener::bind(("0.0.0.0", port)) {
            Ok(_) => {
                break;
            }
            Err(e) => {
                port += 1;
                max_retries -= 1;
                if max_retries == 0 {
                    return Err(e.into());
                }
            }
        }
    }

    Ok(port)
}

pub fn connect_retry(uri: &str, max_retry: usize) -> anyhow::Result<TcpStream> {
    let mut retry = max_retry;
    let mut sleep_time = std::time::Duration::from_millis(5);
    loop {
        match TcpStream::connect(uri) {
            Ok(controller) => {
                return Ok(controller);
            }
            Err(e) => {
                if retry == 0 {
                    return Err(anyhow::anyhow!(
                        "failed to connect to {} after {} retries: {}",
                        uri,
                        max_retry,
                        e
                    ));
                }
                std::thread::sleep(sleep_time);
                sleep_time *= 2;
                retry -= 1;
            }
        }
    }
}
