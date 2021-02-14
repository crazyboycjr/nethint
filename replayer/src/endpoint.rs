use crate::{buffer::Buffer, message};
use std::collections::VecDeque;
use mio::net::TcpStream;
use std::convert::TryInto;
use std::io::{Read, Write};

pub struct Endpoint {
    stream: TcpStream,
    tx_queue: VecDeque<Buffer>,
    state: ReceiveState,
    msg_len: Buffer,
    msg_payload: Buffer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiveState {
    RecvLength,
    RecvPayload,
}

impl Endpoint {
    pub fn new(stream: std::net::TcpStream) -> Self {
        Endpoint {
            stream: TcpStream::from_stream(stream).unwrap(),
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

    pub fn post(&mut self, cmd: message::Command) -> anyhow::Result<()> {
        log::trace!("post a cmd: {:?}", cmd);
        let buf = bincode::serialize(&cmd)?;
        let buf_len = (buf.len() as u64).to_be_bytes();
        self.tx_queue.push_back(Buffer::from_vec(buf_len.into()));
        self.tx_queue.push_back(Buffer::from_vec(buf));
        Ok(())
    }

    pub fn on_send_ready(&mut self) -> anyhow::Result<()> {
        while let Some(buffer) = self.tx_queue.front_mut() {
            if buffer.is_clear() {
                self.tx_queue.pop_front();
                continue;
            }

            let buf = buffer.get_remain_buffer();
            log::trace!("on_send_ready: buf.len: {}", buf.len());
            let nbytes = self.stream.write(buf)?;
            log::trace!("on_send_ready: wrote nbytes: {}", nbytes);
            buffer.mark_handled(nbytes);
        }

        Ok(())
    }

    pub fn on_recv_ready(&mut self) -> anyhow::Result<message::Command> {
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
        let cmd = bincode::deserialize(msg)?;
        log::trace!("on_recv_ready: cmd: {:?}", cmd);

        Ok(cmd)
    }

    fn recv_msg_length(&mut self) -> anyhow::Result<()> {
        let buf = self.msg_len.get_remain_buffer_mut();
        log::trace!("recv_msg_length buf.len: {}", buf.len());
        let nbytes = self.stream.read(buf)?;
        log::trace!("recv_msg_length read nbytes: {}", nbytes);
        self.msg_len.mark_handled(nbytes);
        assert!(nbytes > 0, "connection has been dropped");

        if self.msg_len.is_clear() {
            // prepare payload
            let payload_len =
                u64::from_be_bytes(self.msg_len.as_slice().try_into().unwrap()) as usize;
            self.msg_payload = Buffer::with_len(payload_len);
            log::trace!("payload_len: {}", payload_len);

            // update state
            self.state = ReceiveState::RecvPayload;
        }

        Ok(())
    }

    fn recv_msg_payload(&mut self) -> anyhow::Result<()> {
        let buf = self.msg_payload.get_remain_buffer_mut();
        log::trace!("recv_msg_payload buf.len: {}", buf.len());
        let nbytes = self.stream.read(buf)?;
        log::trace!("recv_msg_payload read nbytes: {}", nbytes);
        self.msg_payload.mark_handled(nbytes);
        assert!(nbytes > 0, "connection has been dropped");

        if self.msg_payload.is_clear() {
            // renew msg_len
            self.msg_len = Buffer::with_len(std::mem::size_of::<u64>());

            // update state
            self.state = ReceiveState::RecvLength;
        }

        Ok(())
    }
}
