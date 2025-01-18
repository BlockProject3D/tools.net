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

use crate::tcp::server::client::ClientTask;
use crate::tcp::util::net::Network;
use crate::tcp::util::DataMsg;
use crate::util::barrier;
use bp3d_debug::{debug, trace};
use std::future::Future;
use std::net::Ipv4Addr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::watch;
use tokio::task::JoinSet;

/// A factory trait which can be used to create the instance of the main server event handler.
pub trait Factory {
    /// The type of the main server handler.
    type Handler: Handler + Send + 'static;

    /// Called when the server is about to start to create the corresponding event handler.
    fn start(self, server: &Arc<Server<Self::Handler>>) -> Self::Handler;
}

/// A trait which represents the main server event handler.
pub trait Handler {
    /// The type of the client event handler.
    type ClientHandler: super::client::Handler + Send + 'static;

    /// The type of request event which can be received by this server event handler.
    type Request: Send + 'static;

    /// The type of reply event which can be sent by this server event handler.
    type Reply: Send + 'static;

    /// Called when a request event was received by the server.
    ///
    /// # Arguments
    ///
    /// * `event`: the received event
    ///
    /// returns: impl Future<Output=()>+Send+Sized
    fn request(&mut self, _: Self::Request) -> impl Future<Output = ()> + Send {
        async move {}
    }

    /// Called when a new client has connected to the server.
    ///
    /// The function is expected to return a new event handler for the given client.
    ///
    /// # Arguments
    ///
    /// * `net`: the network context created for this client.
    ///
    /// returns: impl Future<Output=Result<Self::ClientHandler, Error>>+Send+Sized
    fn connect(
        &mut self,
        net: &mut Network,
    ) -> impl Future<Output = std::io::Result<Self::ClientHandler>> + Send;

    /// Called when a client has disconnected from the server.
    ///
    /// At this point, attempting to recv or send data to the client will most likely result in IO
    /// errors.
    ///
    /// # Arguments
    ///
    /// * `net`: the network context associated to the client about to disconnect.
    /// * `handler`: the client event handler which was associated with the client.
    ///
    /// returns: impl Future<Output=()>+Send+Sized
    fn disconnect(
        &mut self,
        _: &mut Network,
        _: Self::ClientHandler,
    ) -> impl Future<Output = ()> + Send {
        async move {}
    }
}

struct ServerTask<H: Handler> {
    handler: H,
    listener: TcpListener,
    exit_receiver: watch::Receiver<()>,
    request_receiver: mpsc::Receiver<H::Request>,
    server: Arc<Server<H>>,
}

impl<H: Handler + Send + 'static> ServerTask<H> {
    pub async fn run(mut self) -> std::io::Result<()> {
        let mut set = JoinSet::new();
        let mut cur_id = 1;
        loop {
            select! {
                res = self.listener.accept() => {
                    let id = cur_id;
                    cur_id += 1;
                    let (mut stream, addr) = res?;
                    let clients = self.server.cur_clients.fetch_add(1, Relaxed);
                    if clients > self.server.max_clients {
                        self.server.cur_clients.fetch_sub(1, Relaxed);
                        stream.shutdown().await?;
                        continue;
                    }
                    let mut net = Network::new(id, stream, addr);
                    let handler = self.handler.connect(&mut net).await?;
                    let motherfuckingrust = self.server.exit.subscribe();
                    let motherfuckingrust1 = self.server.broadcast.subscribe();
                    debug!({?net}, "Client connected");
                    set.spawn(async move {
                        let task = ClientTask {
                            net: &mut net,
                            handler,
                            exit: motherfuckingrust,
                            broadcast: motherfuckingrust1
                        };
                        let handler = task.run().await;
                        (net, handler)
                    });
                },
                Some(event) = self.request_receiver.recv() => self.handler.request(event).await,
                _ = self.exit_receiver.changed() => break,
                Some(res) = set.join_next() => {
                    self.server.cur_clients.fetch_sub(1, Relaxed);
                    match res {
                        Ok((mut net, handler)) => {
                            debug!({?net}, "Client disconnected");
                            self.handler.disconnect(&mut net, handler).await;
                        },
                        Err(e) => debug!("Client task has crashed: {}", e)
                    }
                }
            }
        }
        set.join_all().await;
        Ok(())
    }
}

/// The main builder structure used to create a new TCP server.
pub struct Builder<F> {
    factory: F,
    max_clients: usize,
    event_queue_size: usize,
}

impl<F> Builder<F> {
    /// Creates a new TCP server from the given main event handler factory.
    pub fn new(factory: F) -> Self {
        Self {
            factory,
            max_clients: 4,
            event_queue_size: 8,
        }
    }

    /// Sets the maximum number of clients allowed at the same time.
    ///
    /// The default is 4.
    ///
    /// # Arguments
    ///
    /// * `count`: max number of clients.
    ///
    /// returns: Builder<F>
    pub fn max_clients(mut self, count: usize) -> Self {
        self.max_clients = count;
        self.event_queue_size(count * 2)
    }

    /// Sets the size of the event queue.
    ///
    /// The default is max_clients * 2.
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
}

impl<F: Factory> Builder<F> {
    /// Bind to ANY IP v4 addresses on the specified port.
    ///
    /// # Arguments
    ///
    /// * `port`: the port to listen on.
    ///
    /// returns: Result<ServerApp<<<F as Factory>::Handler as Handler>::Event>, Error>
    ///
    /// # Errors
    ///
    /// Returns an IO error if the server could not be bound or started.
    pub async fn bind_port(self, port: u16) -> std::io::Result<ServerApp<F::Handler>> {
        self.bind((Ipv4Addr::UNSPECIFIED, port)).await
    }

    /// Bind to localhost on the specified port.
    ///
    /// # Arguments
    ///
    /// * `port`: the port to listen on.
    ///
    /// returns: Result<ServerApp<<<F as Factory>::Handler as Handler>::Event>, Error>
    ///
    /// # Errors
    ///
    /// Returns an IO error if the server could not be bound or started.
    pub async fn bind_local_port(self, port: u16) -> std::io::Result<ServerApp<F::Handler>> {
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
    pub async fn bind(self, addr: impl ToSocketAddrs) -> std::io::Result<ServerApp<F::Handler>> {
        let listener = TcpListener::bind(addr).await?;
        let (exit_sender, exit_receiver) = watch::channel(());
        let brd_sender = barrier::broadcast::barrier(self.max_clients);
        let (request_sender, request_receiver) = mpsc::channel(self.event_queue_size);
        let (reply_sender, reply_receiver) = mpsc::channel(self.event_queue_size);
        let server = Arc::new(Server {
            request_sender,
            broadcast: brd_sender,
            exit: exit_sender,
            max_clients: self.max_clients,
            cur_clients: AtomicUsize::new(0),
            reply_sender,
        });
        let handler = self.factory.start(&server);
        let motherfuckingrust = server.clone();
        let handle = tokio::spawn(async move {
            let task = ServerTask {
                handler,
                listener,
                exit_receiver,
                request_receiver,
                server,
            };
            task.run().await
        });
        Ok(ServerApp {
            handle,
            server: motherfuckingrust,
            reply_receiver,
        })
    }
}

/// Represents a running server.
pub struct Server<H: Handler> {
    exit: watch::Sender<()>,
    broadcast: barrier::broadcast::Sender<DataMsg>,
    cur_clients: AtomicUsize,
    max_clients: usize,
    request_sender: mpsc::Sender<H::Request>,
    reply_sender: mpsc::Sender<H::Reply>,
}

impl<H: Handler> Server<H> {
    /// Returns the maximum number of clients allowed at the same time.
    pub fn max_clients(&self) -> usize {
        self.max_clients
    }

    /// Returns the current number of clients connected.
    pub fn cur_clients(&self) -> usize {
        self.cur_clients.load(Relaxed)
    }

    /// Requests exit of the server.
    pub fn exit(&self) {
        self.broadcast.close();
        let _ = self.exit.send(());
    }

    /// Send a request to the main server event handler from asynchronous code.
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
    pub async fn request_async(&self, event: H::Request) -> Result<(), SendError<H::Request>> {
        self.request_sender.send(event).await
    }

    /// Send a request to the main server event handler from synchronous code.
    ///
    /// # Arguments
    ///
    /// * `event`: the event to send.
    ///
    /// returns: Result<(), TrySendError<E>>
    ///
    /// # Errors
    ///
    /// Returns a TrySendError if the server has exited or if the event queue is full.
    /// See [Builder] for more information on the configuration of the event queue.
    pub fn request(&self, event: H::Request) -> Result<(), TrySendError<H::Request>> {
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
    pub async fn reply(&self, event: H::Reply) -> Result<(), SendError<H::Reply>> {
        self.reply_sender.send(event).await
    }

    /// Send the given data buffer and flush a client stream.
    ///
    /// # Arguments
    ///
    /// * `net_id`: the network id matching the client to send to.
    /// * `msg`: the message buffer to send.
    ///
    /// returns: true if the operation has succeeded, false otherwise.
    pub async fn send(&self, net_id: usize, msg: &[u8]) -> Result<(), barrier::Error> {
        // SAFETY: It is safe to pass a pointer to msg thanks to the barrier synchronization.
        self.broadcast
            .send(DataMsg {
                buffer: msg.as_ptr(),
                buffer_size: msg.len(),
                net_id,
            })
            .await?;
        trace!("All clients have acknowledged");
        Ok(())
    }

    /// Broadcast the given data buffer to all clients.
    ///
    /// # Arguments
    ///
    /// * `msg`: the message buffer to broadcast.
    ///
    /// returns: true if the operation has succeeded, false otherwise.
    pub async fn broadcast(&self, msg: &[u8]) -> Result<(), barrier::Error> {
        // SAFETY: It is safe to pass a pointer to msg thanks to the barrier synchronization.
        self.broadcast
            .send(DataMsg {
                buffer: msg.as_ptr(),
                buffer_size: msg.len(),
                net_id: 0,
            })
            .await?;
        trace!("All clients have acknowledged");
        Ok(())
    }
}

/// The main server application type.
pub type ServerApp<H> = crate::util::ServerApp<Server<H>, <H as Handler>::Reply>;
