// Copyright (c) 2025, BlockProject 3D
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of BlockProject 3D nor the names of its contributors
//       may be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

//! TCP network stream async reader/writer tools.

use crate::tcp::util::buffer::Bytes;
use bp3d_debug::warning;
use std::fmt::{Debug, Formatter};
use std::io::{Error, ErrorKind, IoSlice};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// The event returned by the ready function in [Network].
pub enum ReadyEvent {
    /// The connection was lost, this indicates to break out of the event loop.
    ConnectionLoss,

    /// No particular event (this is used to handle false positives related to OS defects, winshit!!)
    None,

    /// Data was submitted to the given ChannelBuffer channel.
    Submitted,
}

/// Buffered reader/writer for a TCP stream.
///
/// Warning: all reads and writes are buffered so make sure to call flush to actually write data.
pub struct Network {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    addr: SocketAddr,
    id: usize,
}

impl Debug for Network {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Network {{ addr: {:?}, id: {:?} }}", self.addr, self.id)
    }
}

impl Network {
    /// Creates a new network context.
    ///
    /// # Arguments
    ///
    /// * `id`: a unique ID to assign to the network context.
    /// * `stream`: the associated TcpStream.
    /// * `addr`: the socket address.
    ///
    /// returns: Network
    pub fn new(id: usize, stream: TcpStream, addr: SocketAddr) -> Network {
        let (reader, writer) = stream.into_split();
        Network {
            id,
            addr,
            reader,
            writer,
        }
    }

    /// Returns the socket address.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    /// Returns the unique network ID.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Waits for a read or error event to appear on the socket.
    ///
    /// # Arguments
    ///
    /// * `buf`: buffer to read into.
    ///
    /// # Errors
    ///
    /// Returns an IO error if the operation failed.
    pub async fn ready_read<const N: usize>(
        &self,
        bytes_sender: &mpsc::Sender<Bytes<N>>,
    ) -> std::io::Result<ReadyEvent> {
        let ev = self
            .reader
            .ready(Interest::ERROR | Interest::READABLE)
            .await?;
        if ev.is_write_closed() || ev.is_read_closed() || ev.is_error() {
            return Ok(ReadyEvent::ConnectionLoss);
        }
        if !ev.is_readable() {
            return Ok(ReadyEvent::None);
        }
        let mut buf = [0; N];
        let res = self.reader.try_read(&mut buf);
        match res {
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    return Ok(ReadyEvent::None);
                }
                Err(e)
            }
            Ok(v) => {
                if let Err(e) = bytes_sender.send(Bytes::new(buf, v)).await {
                    warning!("ChannelBuffer prematurely closed: {}", e);
                    return Ok(ReadyEvent::ConnectionLoss);
                }
                Ok(ReadyEvent::Submitted)
            }
        }
    }
}

impl AsyncRead for Network {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        unsafe { self.map_unchecked_mut(|v| &mut v.reader).poll_read(cx, buf) }
    }
}

impl AsyncWrite for Network {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        unsafe {
            self.map_unchecked_mut(|v| &mut v.writer)
                .poll_write(cx, buf)
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        unsafe { self.map_unchecked_mut(|v| &mut v.writer).poll_flush(cx) }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        unsafe { self.map_unchecked_mut(|v| &mut v.writer).poll_shutdown(cx) }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, Error>> {
        unsafe {
            self.map_unchecked_mut(|v| &mut v.writer)
                .poll_write_vectored(cx, bufs)
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.writer.is_write_vectored()
    }
}
