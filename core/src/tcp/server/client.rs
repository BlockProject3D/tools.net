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

use crate::tcp::buffer::{Bytes, ChannelBuffer};
use crate::tcp::util::{DataMsg, Network, ReadyEvent};
use crate::tcp::{NetReceiver, BYTES_BUFFER_SIZE, BYTES_CHANNEL_SIZE};
use bp3d_debug::{error, trace, warning};
use std::future::Future;
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;

/// Represents a client event handler.
pub trait Handler {
    /// Called when data is pending to be received from the socket.
    ///
    /// # Arguments
    ///
    /// * `net`: the network receiver context.
    ///
    /// returns: impl Future<Output=Result<(), Error>>+Send+Sized
    fn recv(&mut self, net: &mut NetReceiver) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Called when the client task is about to return.
    ///
    /// # Arguments
    ///
    /// * `net`: the network context.
    ///
    /// returns: impl Future<Output=Result<(), Error>>+Send+Sized
    fn disconnect(&mut self, _: &mut Network) -> impl Future<Output = std::io::Result<()>> + Send {
        async move { Ok(()) }
    }
}

pub(crate) struct ClientTask<'a, H> {
    pub(crate) net: &'a mut Network,
    pub(crate) handler: H,
    pub(crate) exit: watch::Receiver<()>,
    pub(crate) broadcast: broadcast::Receiver<DataMsg>,
}

impl<'a, H: Handler + Send + 'static> ClientTask<'a, H> {
    pub(crate) async fn run(mut self) -> H {
        let net_id = self.net.id();
        let addr = *self.net.addr();
        let (bytes_sender, bytes_receiver) = mpsc::channel(BYTES_CHANNEL_SIZE);
        let handle = tokio::spawn(async move {
            let mut net = NetReceiver::new(ChannelBuffer::new(bytes_receiver), addr, net_id);
            if let Err(e) = self.handler.recv(&mut net).await {
                error!({?net}, "Client error: {}", e);
            }
            net.channel_buffer.close();
            self.handler
        });
        let mut buf = [0; BYTES_BUFFER_SIZE];
        loop {
            select! {
                Ok(event) = self.net.ready(&mut buf) => {
                    match event {
                        ReadyEvent::ConnectionLoss => break,
                        ReadyEvent::None => continue,
                        ReadyEvent::Read(v) => {
                            if let Err(e) = bytes_sender.send(Bytes::new(buf, v)).await {
                                warning!("ChannelBuffer prematurely closed: {}", e);
                                break;
                            }
                        }
                    }
                },
                Ok(msg) = self.broadcast.recv() => unsafe {
                    if let Err(e) = handle_broadcast(msg, self.net).await {
                        error!({net=?self.net}, "Client network error: {}", e);
                        break;
                    }
                },
                _ = self.exit.changed() => break
            }
        }
        drop(bytes_sender);
        let mut handler = handle.await.unwrap();
        if let Err(e) = handler.disconnect(self.net).await {
            error!({net=?self.net}, "Client error: {}", e);
        }
        handler
    }
}

/// SAFETY: DataMsg must point to valid memory (normally ensured by Server structure).
async unsafe fn handle_broadcast(msg: DataMsg, net: &mut Network) -> std::io::Result<()> {
    trace!({?net} {?msg}, "Received broadcast event");
    if msg.net_id == 0 || msg.net_id == net.id() {
        let slice = std::slice::from_raw_parts(msg.buffer, msg.buffer_size);
        net.write_all(slice).await?;
        net.flush().await?;
    }
    (*msg.synchro).add_permits(1);
    Ok(())
}
