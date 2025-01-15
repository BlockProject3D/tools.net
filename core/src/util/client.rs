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

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Represents a client application.
pub struct ClientApp<Client, Reply> {
    pub(crate) handle: JoinHandle<std::io::Result<()>>,
    pub(crate) client: Arc<Client>,
    pub(crate) reply_receiver: mpsc::Receiver<Reply>,
}

impl<Client, Reply> ClientApp<Client, Reply> {
    /// Join and waits for the client to stop.
    ///
    /// Warning this does not automatically exit the client and will wait for a future call to the
    /// [Client::exit] function before returning.
    pub async fn join(self) -> std::io::Result<()> {
        self.handle.await?
    }

    /// Returns the underlying client.
    pub fn client(&self) -> &Arc<Client> {
        &self.client
    }

    /// Receive an event from the main client event handler from asynchronous code.
    ///
    /// Returns None when the channel is closed.
    ///
    /// returns: Option<E>
    pub async fn get_reply_async(&mut self) -> Option<Reply> {
        self.reply_receiver.recv().await
    }

    /// Receive an event from the main client event handler from synchronous code.
    ///
    /// Returns None when the channel is closed or empty.
    ///
    /// returns: Option<E>
    pub fn get_reply(&mut self) -> Option<Reply> {
        self.reply_receiver.try_recv().ok()
    }
}
