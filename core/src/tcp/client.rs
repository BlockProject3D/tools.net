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

//! A basic TCP client implementation designed for long-running connections.

use tokio::sync::{mpsc, watch, Semaphore};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use bp3d_debug::{error, trace};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::select;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use crate::tcp::{NetReceiver, BYTES_CHANNEL_SIZE};
use crate::tcp::buffer::ChannelBuffer;
use crate::tcp::util::{DataMsg, Network, ReadyEvent};

/// The reader trait which is supposed to handle the actual data reading loop.
pub trait Reader {
    /// Called when data is pending to be received from the socket.
    ///
    /// # Arguments
    ///
    /// * `net`: the network receiver context.
    ///
    /// returns: impl Future<Output=Result<(), Error>>+Send+Sized
    fn recv(&mut self, net: &mut NetReceiver) -> impl Future<Output = std::io::Result<()>> + Send;
}

/// Represents the main event handler for a TCP client.
pub trait Handler {
    /// The type of the reader.
    type Reader: Reader + Send + 'static;

    /// The type of request event which can be received by this event handler.
    type Request: Send + 'static;

    /// The type of reply event which can be sent by this event handler.
    type Reply: Send + 'static;

    /// Called when a request event was received by the client.
    ///
    /// # Arguments
    ///
    /// * `event`: the received event.
    /// * `net`: the network context.
    ///
    /// returns: impl Future<Output=()>+Send+Sized
    fn request(&mut self, _: Self::Request, _: &mut Network) -> impl Future<Output = ()> + Send {
        async move {}
    }

    /// Called when the client task has connected to the server.
    ///
    /// The function is expected to return a new event handler for the given client.
    ///
    /// # Arguments
    ///
    /// * `net`: the network context created for this client.
    ///
    /// returns: impl Future<Output=Result<Self::Reader, Error>>+Send+Sized
    fn connect(&mut self, net: &mut Network) -> impl Future<Output = std::io::Result<Self::Reader>> + Send;

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

/// A factory trait which can be used to create the instance of the main event handler.
pub trait Factory {
    /// The type of the main event handler.
    type Handler: Handler + Send + 'static;

    /// Called when the client is about to start to create the corresponding event handler.
    fn start(self, client: &Arc<Client<Self::Handler>>) -> Self::Handler;
}

/// SAFETY: DataMsg must point to valid memory (normally ensured by Client structure).
async unsafe fn handle_data(msg: DataMsg, net: &mut Network) -> std::io::Result<()> {
    trace!({?net} {?msg}, "Received data event");
    let slice = std::slice::from_raw_parts(msg.buffer, msg.buffer_size);
    net.write_all(slice).await?;
    net.flush().await?;
    (*msg.synchro).add_permits(1);
    Ok(())
}

/// The main builder structure used to create a new TCP client.
pub struct Builder<F> {
    factory: F,
    event_queue_size: usize
}

impl<F: Factory> Builder<F> {
    /// Creates a new TCP client from the given main event handler factory.
    pub fn new(factory: F) -> Builder<F> {
        Self {
            factory,
            event_queue_size: 4
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

    /// Connects to the server with the specified address.
    ///
    /// # Arguments
    ///
    /// * `addr`: the address to connect to.
    ///
    /// returns: Result<ClientApp<<<F as Factory>::Handler as Handler>::Event>, Error>
    ///
    /// # Errors
    ///
    /// Returns an IO error if the client could not connect to the specified server.
    pub async fn connect(self, addr: impl ToSocketAddrs) -> std::io::Result<ClientApp<F::Handler>> {
        let stream = TcpStream::connect(addr).await?;
        let addr = stream.peer_addr()?;
        let mut net = Network::new(0, stream, addr);
        let (request_sender, mut request_receiver) = mpsc::channel(self.event_queue_size);
        let (reply_sender, reply_receiver) = mpsc::channel(self.event_queue_size);
        let (exit_sender, mut exit_receiver) = watch::channel(());
        let (data_sender, mut data_receiver) = mpsc::channel(self.event_queue_size);
        let client = Arc::new(Client {
            exit: exit_sender,
            request_sender,
            reply_sender,
            data: data_sender,
            is_exiting: AtomicBool::new(false),
        });
        let mut handler = self.factory.start(&client);
        let handle = tokio::spawn(async move {
            let net_id = net.id();
            let addr = *net.addr();
            let (bytes_sender, bytes_receiver) = mpsc::channel(BYTES_CHANNEL_SIZE);
            let mut reader = handler.connect(&mut net).await?;
            let handle = tokio::spawn(async move {
                let mut net = NetReceiver::new(ChannelBuffer::new(bytes_receiver), addr, net_id);
                if let Err(e) = reader.recv(&mut net).await {
                    error!({?net}, "Client error: {}", e);
                }
                net.channel_buffer.close();
            });
            loop {
                select! {
                    Ok(event) = net.ready_read(&bytes_sender) => {
                        match event {
                            ReadyEvent::ConnectionLoss => break,
                            ReadyEvent::None | ReadyEvent::Submitted => continue,
                        }
                    },
                    _ = exit_receiver.changed() => break,
                    Some(event) = request_receiver.recv() => handler.request(event, &mut net).await,
                    Some(msg) = data_receiver.recv() => unsafe {
                        handle_data(msg, &mut net).await?
                    }
                }
            }
            drop(bytes_sender);
            handle.await?;
            handler.disconnect(&mut net).await?;
            Ok(())
        });
        Ok(ClientApp {
            client,
            handle,
            reply_receiver
        })
    }
}

/// Represents a client with a long-running connection.
pub struct Client<H: Handler> {
    exit: watch::Sender<()>,
    request_sender: mpsc::Sender<H::Request>,
    data: mpsc::Sender<DataMsg>,
    reply_sender: mpsc::Sender<H::Reply>,
    is_exiting: AtomicBool
}

impl<H: Handler> Client<H> {
    /// Requests exit of the client.
    pub fn exit(&self) {
        self.is_exiting.store(true, Relaxed);
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
    pub async fn request_async(&self, event: H::Request) -> Result<(), SendError<H::Request>> {
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

    /// Send the given data buffer and flush the stream.
    ///
    /// # Arguments
    ///
    /// * `msg`: the message buffer to send.
    ///
    /// returns: true if the operation has succeeded, false otherwise.
    pub async fn send(&self, msg: &[u8]) -> Result<(), crate::tcp::util::SendError> {
        if self.is_exiting.load(Relaxed) {
            return Err(crate::tcp::util::SendError::IsExiting);
        }
        // SAFETY: It is safe to pass a pointer to msg as long as we wait for all clients to have
        // consumed the pointer before returning.
        let synchro = Semaphore::new(0);
        if (self.data.send(DataMsg {
            synchro: &synchro,
            buffer: msg.as_ptr(),
            buffer_size: msg.len(),
            net_id: 0
        }).await).is_err() {
            return Err(crate::tcp::util::SendError::Closed);
        }
        trace!("Waiting for the async task to acknowledge");
        let _ = synchro.acquire().await.unwrap();
        Ok(())
    }
}

/// The main client application type.
pub type ClientApp<H: Handler> = crate::util::ClientApp<Client<H>, H::Reply>;
