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

//! Basic message based IPC implementation.

#[cfg(unix)]
mod unix;
pub mod util;

#[cfg(unix)]
type ServerImpl = unix::Server;
#[cfg(unix)]
type ClientImpl = unix::Client;

/// Represents an IPC Server.
#[derive(Debug)]
pub struct Server(ServerImpl);

impl Server {
    /// Creates a new IPC Server with the given name for clients to connect to.
    ///
    /// An IPC [Server](Server) accepts 1 or more local-only clients and can exchange data with any
    /// of its connected clients. This facility transports messages instead of bytes unlike other
    /// methods relying on mkfifo or SOCK_STREAM.
    ///
    /// # Platform specific
    ///
    /// This function automatically releases all system resources on drop.
    ///
    /// This is implemented using UNIX domain sockets (SOCK_DGRAM mode) under all unixes and using
    /// named pipes on windows.
    ///
    /// # Arguments
    ///
    /// * `name`: the name of the [Server](Server) which clients will use when connecting.
    ///
    /// returns: Result<Server, Error>
    pub async fn create(name: &str) -> std::io::Result<Self> {
        ServerImpl::create(name).await.map(Self)
    }

    /// Accepts a new client.
    pub async fn accept(&self) -> std::io::Result<Client> {
        self.0.accept().await.map(Client)
    }
}

/// Represents an IPC peer either accepted from an existing IPC [Server](Server) or a standalone
/// connection using [open](Client::open).
#[derive(Debug)]
pub struct Client(ClientImpl);

impl Client {
    /// Opens a connection with an existing IPC [Server](Server).
    ///
    /// # Arguments
    ///
    /// * `name`: the name of the server to connect to.
    ///
    /// returns: Result<Client, Error>
    pub async fn open(name: &str) -> std::io::Result<Self> {
        ClientImpl::open(name).await.map(Self)
    }

    /// Sends the given message to the other peer.
    ///
    /// Returns the number of bytes written.
    ///
    /// # Arguments
    ///
    /// * `msg`: the message to send.
    ///
    /// returns: Result<usize, Error>
    pub async fn send(&self, msg: &util::Message) -> std::io::Result<usize> {
        self.0.send(msg).await
    }

    /// Attempts to receive one message from the other peer.
    ///
    /// # Arguments
    ///
    /// * `msg`: the message to fill in.
    ///
    /// returns: Result<(), Error>
    pub async fn recv(&self, msg: &mut util::Message) -> std::io::Result<()> {
        self.0.recv(msg).await
    }

    /// This function will gracefully close the connection with the other peer which should return
    /// a message of size 0 received on the other end.
    pub async fn close(self) -> std::io::Result<()> {
        self.0.close().await
    }
}
