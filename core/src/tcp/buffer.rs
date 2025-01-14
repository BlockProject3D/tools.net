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

use std::fmt::{Debug, Formatter};
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc;
use crate::tcp::BYTES_BUFFER_SIZE;

pub struct Bytes<const N: usize> {
    bytes: [u8; N],
    size: usize,
}

impl<const N: usize> Bytes<N> {
    pub fn new(bytes: [u8; N], size: usize) -> Self {
        Self { bytes, size }
    }
}

impl<const N: usize> Deref for Bytes<N> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes[..self.size]
    }
}

pub struct ChannelBuffer<const N: usize> {
    receiver: mpsc::Receiver<Bytes<N>>
}

impl<const N: usize> ChannelBuffer<N> {
    pub fn new(receiver: mpsc::Receiver<Bytes<N>>) -> Self {
        Self { receiver }
    }

    pub fn close(mut self) {
        self.receiver.close();
    }
}

impl<const N: usize> AsyncRead for ChannelBuffer<N> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let msg = self.receiver.poll_recv(cx);
        match msg {
            Poll::Ready(v) => {
                match v {
                    Some(bytes) => {
                        buf.put_slice(&bytes);
                        Poll::Ready(Ok(()))
                    }
                    None => Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, "channel buffer is closed"))),
                }
            }
            Poll::Pending => Poll::Pending
        }
    }
}

/// Represents a network receiver.
pub struct NetReceiver {
    pub(super) channel_buffer: ChannelBuffer<BYTES_BUFFER_SIZE>,
    addr: SocketAddr,
    id: usize
}

impl Debug for NetReceiver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NetReceiver {{ addr: {:?}, id: {:?} }}", self.addr, self.id)
    }
}

impl NetReceiver {
    /// Creates a new instance of a NetReceiver.
    ///
    /// # Arguments
    ///
    /// * `channel_buffer`: the channel buffer to read bytes from.
    /// * `addr`: the peer address.
    /// * `id`: the network id associated to the client.
    ///
    /// returns: NetReceiver
    pub fn new(channel_buffer: ChannelBuffer<BYTES_BUFFER_SIZE>, addr: SocketAddr, id: usize) -> Self {
        Self { channel_buffer, addr, id }
    }

    /// Returns the socket address.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    /// Returns the unique network ID.
    pub fn id(&self) -> usize {
        self.id
    }
}

impl AsyncRead for NetReceiver {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        unsafe { self.map_unchecked_mut(|v| &mut v.channel_buffer).poll_read(cx, buf) }
    }
}
