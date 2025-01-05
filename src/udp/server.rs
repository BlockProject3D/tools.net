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

//! A basic UDP server implementation with support for receiving datagrams in loop.

use std::future::Future;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::select;
use tokio::sync::watch;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::task::JoinHandle;
use crate::udp::util::Datagram;

/// A trait which represents the main server event handler.
pub trait Handler {
    /// The type of event which can be received by this server event handler.
    type Event: Send + 'static;

    /// Called when a datagram was received from the socket.
    ///
    /// # Arguments
    ///
    /// * `server`: an instance of the server which the socket is attached to.
    /// * `datagram`: the received datagram.
    ///
    /// returns: impl Future<Output=Result<(), Error>>+Send+Sized
    fn recv(&mut self, server: &Server<Self::Event>, datagram: Datagram) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Called when an event was received by the server.
    ///
    /// # Arguments
    ///
    /// * `server`: the server which received the event.
    /// * `event`: the received event.
    ///
    /// returns: impl Future<Output=()>+Send+Sized
    fn event(&mut self, _: &Server<Self::Event>, _: Self::Event) -> impl Future<Output = ()> + Send {
        async move { }
    }
}

struct ServerTask<H: Handler, const N: usize> {
    server: Arc<Server<H::Event>>,
    exit_receiver: watch::Receiver<()>,
    event_receiver: mpsc::Receiver<H::Event>,
    buffer: [u8; N],
    handler: H
}

impl<H: Handler + Send + 'static, const N: usize> ServerTask<H, N> {
    pub async fn run(mut self) -> std::io::Result<()> {
        loop {
            select! {
                _ = self.exit_receiver.changed() => break,
                res = self.server.socket.recv_from(&mut self.buffer) => {
                    let (len, addr) = res?;
                    self.handler.recv(&self.server, Datagram::new(addr, &self.buffer[..len])).await?;
                },
                Some(event) = self.event_receiver.recv() => self.handler.event(&self.server, event).await
            }
        }
        Ok(())
    }
}

/// The main builder structure used to create a new UDP server.
pub struct Builder<H, const N: usize> {
    handler: H,
    event_queue_size: usize,
    init_buffer: [u8; N]
}

impl<H: Handler + Send + 'static, const N: usize> Builder<H, N> {
    /// Creates a new UDP server from the given main event handler.
    pub fn new(handler: H, init_buffer: [u8; N]) -> Builder<H, N> {
        Self {
            handler,
            event_queue_size: 4,
            init_buffer
        }
    }

    /// Sets the size of the event queue.
    ///
    /// The default is 4.
    ///
    /// # Arguments
    ///
    /// * `size`: event queue size.
    ///
    /// returns: Builder<F>
    pub fn event_queue_size(mut self, size: usize) -> Self {
        self.event_queue_size = size;
        self
    }

    /// Bind to ANY IP v4 addresses on the specified port.
    ///
    /// # Arguments
    ///
    /// * `port`: the port to listen on.
    ///
    /// returns: Result<ServerApp<H::Event>, Error>
    ///
    /// # Errors
    ///
    /// Returns an IO error if the server could not be bound or started.
    pub async fn bind_port(self, port: u16) -> std::io::Result<ServerApp<H::Event>> {
        self.bind((Ipv4Addr::UNSPECIFIED, port)).await
    }

    /// Bind to localhost on the specified port.
    ///
    /// # Arguments
    ///
    /// * `port`: the port to listen on.
    ///
    /// returns: Result<ServerApp<H::Event>, Error>
    ///
    /// # Errors
    ///
    /// Returns an IO error if the server could not be bound or started.
    pub async fn bind_local_port(self, port: u16) -> std::io::Result<ServerApp<H::Event>> {
        self.bind((Ipv4Addr::LOCALHOST, port)).await
    }

    /// Bind to an address.
    ///
    /// # Arguments
    ///
    /// * `addr`: the address to bind to.
    ///
    /// returns: Result<ServerApp<<<F as Factory>::Handler as Handler>::Event>, Error>
    ///
    /// # Errors
    ///
    /// Returns an IO error if the server could not be bound or started.
    pub async fn bind(self, addr: impl ToSocketAddrs) -> std::io::Result<ServerApp<H::Event>> {
        let socket = UdpSocket::bind(addr).await?;
        let (exit_sender, exit_receiver) = watch::channel(());
        let (event_sender, event_receiver) = mpsc::channel(self.event_queue_size);
        let server = Arc::new(Server {
            socket,
            exit: exit_sender,
            event_sender
        });
        let motherfuckingrust = server.clone();
        let handle = tokio::spawn(async move {
            let task = ServerTask {
                server,
                exit_receiver,
                event_receiver,
                buffer: self.init_buffer,
                handler: self.handler
            };
            task.run().await
        });
        Ok(ServerApp {
            server: motherfuckingrust,
            handle
        })
    }
}

/// Represents a running server.
pub struct Server<E> {
    socket: UdpSocket,
    exit: watch::Sender<()>,
    event_sender: mpsc::Sender<E>
}

impl<E> Server<E> {
    /// Requests exit of the server.
    pub fn exit(&self) {
        let _ = self.exit.send(());
    }

    /// Send an event to the main event handler from asynchronous code.
    ///
    /// # Arguments
    ///
    /// * `event`: the event to send.
    ///
    /// returns: Result<(), SendError<E>>
    ///
    /// # Errors
    ///
    /// Returns a SendError if the server has exited.
    pub async fn event_async(&self, event: E) -> Result<(), SendError<E>> {
        self.event_sender.send(event).await
    }

    /// Send an event to the main event handler from synchronous code.
    ///
    /// # Arguments
    ///
    /// * `event`:
    ///
    /// returns: Result<(), TrySendError<E>>
    ///
    /// # Errors
    ///
    /// Returns a TrySendError if the server has exited or if the event queue is full.
    /// See [Builder] for more information on the configuration of the event queue.
    pub fn event(&self, event: E) -> Result<(), TrySendError<E>> {
        self.event_sender.try_send(event)
    }

    /// Send a datagram to a peer from this server socket.
    ///
    /// # Arguments
    ///
    /// * `peer_addr`: the address of the peer intended to receive the datagram.
    /// * `data`: the data to send.
    ///
    /// returns: Result<usize, Error>
    pub async fn send(&self, peer_addr: impl ToSocketAddrs, data: &[u8]) -> std::io::Result<usize> {
        self.socket.send_to(data, peer_addr).await
    }
}

/// Represents a server application.
pub struct ServerApp<E> {
    server: Arc<Server<E>>,
    handle: JoinHandle<std::io::Result<()>>
}

impl<E> ServerApp<E> {
    /// Join and waits for the server to stop.
    ///
    /// Warning: this does not automatically exit the server and will wait for a future call to the
    /// [Server::exit] function before returning.
    pub async fn join(self) -> std::io::Result<()> {
        self.handle.await?
    }
}
