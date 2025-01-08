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

use std::future::Future;
use bp3d_debug::trace;
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::sync::watch;
use tokio::sync::broadcast;
use crate::tcp::util::{DataMsg, Network};

/// Represents a client event handler.
pub trait Handler {
    /// Called when data is pending to be received from the socket.
    ///
    /// # Arguments
    ///
    /// * `net`: the network context.
    ///
    /// returns: impl Future<Output=Result<(), Error>>+Send+Sized
    fn recv(&mut self, net: &mut Network) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Called when the client task is about to return.
    ///
    /// # Arguments
    ///
    /// * `net`: the network context.
    ///
    /// returns: impl Future<Output=Result<(), Error>>+Send+Sized
    fn disconnect(&mut self, _: &mut Network) -> impl Future<Output = std::io::Result<()>> + Send {
        async move {
            Ok(())
        }
    }
}

pub(crate) struct ClientTask<'a, H> {
    pub(crate) net: &'a mut Network,
    pub(crate) handler: &'a mut H,
    pub(crate) exit: watch::Receiver<()>,
    pub(crate) broadcast: broadcast::Receiver<DataMsg>
}

impl<'a, H: Handler + Send + 'static> ClientTask<'a, H> {
    pub(crate) async fn run(mut self) -> std::io::Result<()> {
        loop {
            select! {
                res = self.net.ready() => {
                    let ev = res?;
                    if ev.is_error() || ev.is_read_closed() || ev.is_write_closed() {
                        break;
                    }
                    if ev.is_readable() {
                        self.handler.recv(self.net).await?;
                    }
                },
                Ok(msg) = self.broadcast.recv() => unsafe {
                    handle_broadcast(msg, self.net).await?
                },
                _ = self.exit.changed() => break
            }
        }
        self.handler.disconnect(self.net).await?;
        Ok(())
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
