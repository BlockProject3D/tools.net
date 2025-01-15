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
use std::sync::Arc;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::watch;

/// A trait which represents the main server event handler.
pub trait Handler {
    /// The type of request event which can be received by this event handler.
    type Request: Send + 'static;

    /// The type of reply event which can be sent by this event handler.
    type Reply: Send + 'static;

    /// Called when a datagram was received from the socket.
    ///
    /// # Arguments
    ///
    /// * `client`: an instance of the server which the socket is attached to.
    /// * `datagram`: the received datagram.
    ///
    /// returns: impl Future<Output=Result<(), Error>>+Send+Sized
    fn recv(
        &mut self,
        client: &Client<Self::Request, Self::Reply>,
        datagram: &[u8],
    ) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Called when a request event was received by the client.
    ///
    /// # Arguments
    ///
    /// * `client`: the client which received the event.
    /// * `event`: the received event.
    ///
    /// returns: impl Future<Output=()>+Send+Sized
    fn request(
        &mut self,
        _: &Client<Self::Request, Self::Reply>,
        _: Self::Request,
    ) -> impl Future<Output = ()> + Send {
        async move {}
    }
}

struct ClientTask<H: Handler, const N: usize> {
    client: Arc<Client<H::Request, H::Reply>>,
    exit_receiver: watch::Receiver<()>,
    request_receiver: mpsc::Receiver<H::Request>,
    buffer: [u8; N],
    handler: H,
}

impl<H: Handler + Send + 'static, const N: usize> ClientTask<H, N> {
    pub async fn run(mut self) -> std::io::Result<()> {
        loop {
            select! {
                _ = self.exit_receiver.changed() => break,
                res = self.client.socket.recv(&mut self.buffer) => {
                    let len = res?;
                    self.handler.recv(&self.client, &self.buffer[..len]).await?;
                },
                Some(event) = self.request_receiver.recv() => self.handler.request(&self.client, event).await
            }
        }
        Ok(())
    }
}

/// The main builder structure used to create a new UDP client.
pub struct Builder<H, const N: usize> {
    handler: H,
    event_queue_size: usize,
    init_buffer: [u8; N],
}

impl<H: Handler + Send + 'static, const N: usize> Builder<H, N> {
    /// Creates a new UDP client from the given main event handler.
    pub fn new(handler: H, init_buffer: [u8; N]) -> Builder<H, N> {
        Self {
            handler,
            event_queue_size: 4,
            init_buffer,
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

    /// Connect to the server at the specified address.
    ///
    /// Warning: as UDP does not have a concept of connection, there is no guarantee that a success
    /// return of this function means the other peer will receive the datagrams at all.
    ///
    /// # Arguments
    ///
    /// * `addr`: the address to bind to.
    ///
    /// returns: Result<ClientApp<H::Event>, Error>
    ///
    /// # Errors
    ///
    /// Returns an IO error if the server could not be bound or started.
    pub async fn connect(
        self,
        addr: impl ToSocketAddrs,
    ) -> std::io::Result<ClientApp<H::Request, H::Reply>> {
        let socket = UdpSocket::bind(addr).await?;
        let (exit_sender, exit_receiver) = watch::channel(());
        let (request_sender, request_receiver) = mpsc::channel(self.event_queue_size);
        let (reply_sender, reply_receiver) = mpsc::channel(self.event_queue_size);
        let client = Arc::new(Client {
            socket,
            exit: exit_sender,
            request_sender,
            reply_sender,
        });
        let motherfuckingrust = client.clone();
        let handle = tokio::spawn(async move {
            let task = ClientTask {
                client,
                exit_receiver,
                request_receiver,
                buffer: self.init_buffer,
                handler: self.handler,
            };
            task.run().await
        });
        Ok(ClientApp {
            client: motherfuckingrust,
            handle,
            reply_receiver,
        })
    }
}

/// Represents a running client.
pub struct Client<E, E2> {
    socket: UdpSocket,
    exit: watch::Sender<()>,
    request_sender: mpsc::Sender<E>,
    reply_sender: mpsc::Sender<E2>,
}

impl<E, E2> Client<E, E2> {
    /// Requests exit of the client.
    pub fn exit(&self) {
        let _ = self.exit.send(());
    }

    /// Send a request to the main event handler from asynchronous code.
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
    pub async fn request_async(&self, event: E) -> Result<(), SendError<E>> {
        self.request_sender.send(event).await
    }

    /// Send a request to the main event handler from synchronous code.
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
    pub fn request(&self, event: E) -> Result<(), TrySendError<E>> {
        self.request_sender.try_send(event)
    }

    /// Sends a reply event to the main application.
    ///
    /// # Arguments
    ///
    /// * `event`: the event to send.
    ///
    /// returns: Result<(), SendError<E2>>
    ///
    /// # Errors
    ///
    /// Returns a SendError if the server has exited.
    pub async fn reply(&self, event: E2) -> Result<(), SendError<E2>> {
        self.reply_sender.send(event).await
    }

    /// Send a datagram to the server.
    ///
    /// # Arguments
    ///
    /// * `data`: the data to send.
    ///
    /// returns: Result<usize, Error>
    pub async fn send(&self, data: &[u8]) -> std::io::Result<usize> {
        self.socket.send(data).await
    }
}

/// The main client application type.
pub type ClientApp<E, E2> = crate::util::ClientApp<Client<E, E2>, E2>;
