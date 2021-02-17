use crate::buffer::Buffer;
use mio::net::TcpStream;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::io::{Read, Write};
use thiserror::Error;
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, Error)]
pub enum Error {
    #[error("WouldBlock")]
    WouldBlock,
    #[error("Connection has been dropped")]
    ConnectionLost,
    #[error("IO Error: {0}")]
    IoError(std::io::Error),
    #[error("Deserialize failed: {0}")]
    Deserialize(bincode::Error),
}

type Result<T> = std::result::Result<T, Error>;

pub struct Endpoint<'p> {
    stream: TcpStream,
    poll: &'p mio::Poll,
    tx_queue: VecDeque<Buffer>,
    state: ReceiveState,
    msg_len: Buffer,
    msg_payload: Buffer,
}

impl<'p> std::ops::Drop for Endpoint<'p> {
    fn drop(&mut self) {
        self.poll.deregister(&self.stream).unwrap()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiveState {
    RecvLength,
    RecvPayload,
}

impl<'p> Endpoint<'p> {
    pub fn new(stream: std::net::TcpStream, poll: &'p mio::Poll) -> Self {
        Endpoint {
            // this will set stream to non-blocking for us
            stream: TcpStream::from_stream(stream).unwrap(),
            poll,
            tx_queue: Default::default(),
            state: ReceiveState::RecvLength,
            msg_len: Buffer::with_len(std::mem::size_of::<u64>()),
            msg_payload: Buffer::default(),
        }
    }

    #[inline]
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    #[inline]
    pub fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    pub fn post(&mut self, cmd: impl Serialize + std::fmt::Debug) -> anyhow::Result<()> {
        log::trace!("post a cmd: {:?}", cmd);
        let buf = bincode::serialize(&cmd)?;
        let buf_len = (buf.len() as u64).to_be_bytes();
        self.tx_queue.push_back(Buffer::from_vec(buf_len.into()));
        self.tx_queue.push_back(Buffer::from_vec(buf));
        Ok(())
    }

    pub fn on_send_ready(&mut self) -> Result<()> {
        while let Some(buffer) = self.tx_queue.front_mut() {
            if buffer.is_clear() {
                self.tx_queue.pop_front();
                continue;
            }

            Self::send_buffer(&mut self.stream, buffer)?;
       }

        Ok(())
    }

    pub fn on_recv_ready<T: DeserializeOwned + std::fmt::Debug>(&mut self) -> Result<T> {
        use ReceiveState::*;
        match self.state {
            RecvLength => {
                self.recv_msg_length()?;
                self.recv_msg_payload()?;
            }
            RecvPayload => {
                self.recv_msg_payload()?;
            }
        }

        // here we get an entire message
        let msg = self.msg_payload.as_slice();
        let cmd = bincode::deserialize(msg).map_err(Error::Deserialize)?;
        log::trace!("on_recv_ready: cmd: {:?}", cmd);

        Ok(cmd)
    }

    fn recv_buffer(stream: &mut TcpStream, buffer: &mut Buffer) -> Result<usize> {
        let buf = buffer.get_remain_buffer_mut();
        match stream.read(buf) {
            Ok(0) => Err(Error::ConnectionLost),
            Ok(nbytes) => {
                buffer.mark_handled(nbytes);
                Ok(nbytes)
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => Err(Error::WouldBlock),
            Err(e) => Err(Error::IoError(e)),
        }
    }

    fn send_buffer(stream: &mut TcpStream, buffer: &mut Buffer) -> Result<usize> {
        let buf = buffer.get_remain_buffer();
        match stream.write(buf) {
            Ok(nbytes) => {
                buffer.mark_handled(nbytes);
                Ok(nbytes)
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => Err(Error::WouldBlock),
            Err(e) => Err(Error::IoError(e)),
        }
    }

    fn recv_msg_length(&mut self) -> Result<()> {
        loop {
            Self::recv_buffer(&mut self.stream, &mut self.msg_len)?;

            if self.msg_len.is_clear() {
                // prepare payload
                let payload_len =
                    u64::from_be_bytes(self.msg_len.as_slice().try_into().unwrap()) as usize;
                self.msg_payload = Buffer::with_len(payload_len);

                // update state
                self.state = ReceiveState::RecvPayload;
                break Ok(());
            }
        }
    }

    fn recv_msg_payload(&mut self) -> Result<()> {
        loop {
            Self::recv_buffer(&mut self.stream, &mut self.msg_payload)?;

            if self.msg_payload.is_clear() {
                // renew msg_len
                self.msg_len = Buffer::with_len(std::mem::size_of::<u64>());

                // update state
                self.state = ReceiveState::RecvLength;
                break Ok(());
            }
        }
    }
}
