use crate::buffer::Buffer;
use crate::Node;
use mio::net::TcpStream;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::VecDeque;
use std::convert::TryInto;
use std::io::{Read, Write};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("WouldBlock")]
    WouldBlock,
    #[error("Connection has been dropped")]
    ConnectionLost,
    #[error("Nothing to send")]
    NothingToSend,
    #[error("IO Error: {0}")]
    IoError(std::io::Error),
    #[error("Deserialize failed: {0}")]
    Deserialize(bincode::Error),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, Default)]
struct MsgLength(u64, u64);

// the length of meta must be known and fixed
static_assertions::assert_eq_size!(MsgLength, [u8; 16]);

impl std::convert::Into<[u8; 16]> for MsgLength {
    fn into(self) -> [u8; 16] {
        unsafe {
            std::mem::transmute::<[[u8; std::mem::size_of::<u64>()]; 2], _>([
                self.0.to_be_bytes(),
                self.1.to_be_bytes(),
            ])
        }
    }
}

impl std::convert::TryFrom<&[u8]> for MsgLength {
    type Error = std::array::TryFromSliceError;
    fn try_from(slice: &[u8]) -> std::result::Result<Self, Self::Error> {
        let payload_len = u64::from_be_bytes(slice[..std::mem::size_of::<u64>()].try_into()?);
        let attach_len = u64::from_be_bytes(slice[std::mem::size_of::<u64>()..].try_into()?);
        Ok(MsgLength(payload_len, attach_len))
    }
}

pub struct Builder {
    stream: Option<std::net::TcpStream>,
    interest: mio::Ready,
    node: Option<Node>,
}

impl Builder {
    pub fn new() -> Self {
        Builder {
            stream: None,
            interest: mio::Ready::empty(),
            node: None,
        }
    }

    pub fn stream(mut self, stream: std::net::TcpStream) -> Self {
        self.stream = Some(stream);
        self
    }

    pub fn readable(mut self, readable: bool) -> Self {
        if readable {
            self.interest |= mio::Ready::readable();
        }
        self
    }

    pub fn writable(mut self, writable: bool) -> Self {
        if writable {
            self.interest |= mio::Ready::writable();
        }
        self
    }

    pub fn node(mut self, node: Node) -> Self {
        self.node = Some(node);
        self
    }

    pub fn build(self) -> anyhow::Result<Endpoint> {
        if self.stream.is_none() {
            return Err(anyhow::anyhow!("empty stream"));
        }
        if self.node.is_none() {
            return Err(anyhow::anyhow!("empty node"));
        }
        Ok(Endpoint::new(
            self.stream.unwrap(),
            self.interest,
            self.node.unwrap(),
        ))
    }
}

pub struct Endpoint {
    stream: TcpStream,
    interest: mio::Ready,
    node: Node,
    // sending states
    tx_queue: VecDeque<SendState>,
    // receiving states
    recv_state: RecvState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecvStage {
    RecvMeta,
    RecvPayload,
    RecvAttachment,
}

#[derive(Debug)]
struct RecvState {
    stage: RecvStage,
    meta: MsgLength,
    meta_buf: Buffer,
    payload: Buffer,
    attachment: Buffer,
}

impl std::default::Default for RecvState {
    fn default() -> Self {
        RecvState {
            stage: RecvStage::RecvMeta,
            meta: MsgLength::default(),
            meta_buf: Buffer::with_len(std::mem::size_of::<MsgLength>()),
            payload: Buffer::default(),
            attachment: Buffer::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendStage {
    SendMeta,
    SendPayload,
    SendAttachment,
    SendDone,
}

struct SendState {
    stage: SendStage,
    meta: Buffer,
    payload: Buffer,
    attachment: Option<Buffer>,
}

impl SendState {
    fn new<T: Serialize + std::fmt::Debug>(
        cmd: T,
        attachment: Option<Vec<u8>>,
    ) -> anyhow::Result<Self> {
        let buf = bincode::serialize(&cmd)?;
        let meta = MsgLength(
            buf.len() as u64,
            attachment.as_ref().map(|x| x.len()).unwrap_or(0) as u64,
        );
        let meta_arr: [u8; 16] = meta.into();

        Ok(SendState {
            stage: SendStage::SendMeta,
            meta: Buffer::from_vec(meta_arr.into()),
            payload: Buffer::from_vec(buf),
            attachment: attachment.map(|a| Buffer::from_vec(a)),
        })
    }
}

impl Endpoint {
    fn new(stream: std::net::TcpStream, interest: mio::Ready, node: Node) -> Self {
        Endpoint {
            // this will set stream to non-blocking for us
            stream: TcpStream::from_stream(stream).unwrap(),
            interest,
            node,
            tx_queue: Default::default(),
            recv_state: RecvState::default(),
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

    #[inline]
    pub fn interest(&self) -> mio::Ready {
        self.interest
    }

    #[inline]
    pub fn node(&self) -> &Node {
        &self.node
    }

    pub fn post(
        &mut self,
        cmd: impl Serialize + std::fmt::Debug,
        attachment: Option<Vec<u8>>,
    ) -> anyhow::Result<()> {
        log::trace!("post a cmd: {:?}", cmd);
        let state = SendState::new(cmd, attachment)?;
        self.tx_queue.push_back(state);
        Ok(())
    }

    // how about first return the attachment, more things to return can be added later
    pub fn on_send_ready(&mut self) -> Result<Option<Vec<u8>>> {
        use SendStage::*;

        if let Some(mut state) = self.tx_queue.front_mut() {
            match state.stage {
                SendMeta => {
                    Self::send_meta(&mut self.stream, &mut state)?;
                    Self::send_payload(&mut self.stream, &mut state)?;
                    Self::send_attachment(&mut self.stream, &mut state)?;
                }
                SendPayload => {
                    Self::send_payload(&mut self.stream, &mut state)?;
                    Self::send_attachment(&mut self.stream, &mut state)?;
                }
                SendAttachment => {
                    Self::send_attachment(&mut self.stream, &mut state)?;
                }
                SendDone => {
                    unreachable!();
                }
            }
        }

        if let Some(state) = self.tx_queue.pop_front() {
            Ok(state.attachment.map(|mut b| b.take()))
        } else {
            Err(Error::NothingToSend)
        }
    }

    fn send_meta(stream: &mut TcpStream, state: &mut SendState) -> Result<()> {
        while !state.meta.is_clear() {
            Self::send_buffer(stream, &mut state.meta)?;
        }
        state.stage = SendStage::SendPayload;
        Ok(())
    }

    fn send_payload(stream: &mut TcpStream, state: &mut SendState) -> Result<()> {
        while !state.payload.is_clear() {
            Self::send_buffer(stream, &mut state.payload)?;
        }
        state.stage = SendStage::SendAttachment;
        Ok(())
    }

    fn send_attachment(stream: &mut TcpStream, state: &mut SendState) -> Result<()> {
        if let Some(attachment) = state.attachment.as_mut() {
            while !attachment.is_clear() {
                Self::send_buffer(stream, attachment)?;
            }
        }
        state.stage = SendStage::SendDone;
        Ok(())
    }

    pub fn on_recv_ready<T: DeserializeOwned + std::fmt::Debug>(
        &mut self,
    ) -> Result<(T, Option<&[u8]>)> {
        use RecvStage::*;
        match self.recv_state.stage {
            RecvMeta => {
                self.recv_meta()?;
                self.recv_payload()?;
                self.recv_attachment()?;
            }
            RecvPayload => {
                self.recv_payload()?;
                self.recv_attachment()?;
            }
            RecvAttachment => {
                self.recv_attachment()?;
            }
        }

        // here we get an entire message
        let msg = self.recv_state.payload.as_slice();
        let cmd = bincode::deserialize(msg).map_err(Error::Deserialize)?;
        log::trace!("on_recv_ready: cmd: {:?}", cmd);

        if self.recv_state.meta.1 != 0 {
            let attachment = self.recv_state.attachment.as_slice();
            Ok((cmd, Some(attachment)))
        } else {
            Ok((cmd, None))
        }
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

    fn recv_meta(&mut self) -> Result<()> {
        loop {
            Self::recv_buffer(&mut self.stream, &mut self.recv_state.meta_buf)?;

            if self.recv_state.meta_buf.is_clear() {
                self.recv_state.meta = self.recv_state.meta_buf.as_slice().try_into().unwrap();
                self.recv_state.payload = Buffer::with_len(self.recv_state.meta.0 as _);

                if self.recv_state.meta.1 != 0 {
                    self.recv_state.attachment = Buffer::with_len(self.recv_state.meta.1 as _);
                }

                // update state
                self.recv_state.stage = RecvStage::RecvPayload;
                break Ok(());
            }
        }
    }

    fn recv_payload(&mut self) -> Result<()> {
        loop {
            Self::recv_buffer(&mut self.stream, &mut self.recv_state.payload)?;

            if self.recv_state.payload.is_clear() {
                // update state
                self.recv_state.stage = RecvStage::RecvAttachment;
                break Ok(());
            }
        }
    }

    fn recv_attachment(&mut self) -> Result<()> {
        while self.recv_state.meta.1 > 0 {
            Self::recv_buffer(&mut self.stream, &mut self.recv_state.attachment)?;

            if self.recv_state.attachment.is_clear() {
                break;
            }
        }

        // renew msg_len
        self.recv_state.meta_buf = Buffer::with_len(std::mem::size_of::<MsgLength>());

        // update state
        self.recv_state.stage = RecvStage::RecvMeta;
        Ok(())
    }
}
