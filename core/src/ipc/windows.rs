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

use crate::ipc::util::Message;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, PipeMode, ServerOptions,
};
use tokio::task::JoinHandle;

const INIT_SERVER_CONNECT: &[u8] = &[0xCD];
const ACK: &[u8] = &[0xFF];
const END: &[u8] = &[0xFE];

const MAX_FAILURES: usize = 2;

#[derive(Debug)]
pub struct Server {
    pipe: NamedPipeServer,
    path: PathBuf,
}

impl Server {
    pub async fn create(name: &str) -> std::io::Result<Server> {
        let path = Path::new("\\\\.\\pipe\\").join(name);
        let pipe = ServerOptions::new()
            .first_pipe_instance(true)
            .pipe_mode(PipeMode::Message)
            .create(&path)?;
        Ok(Self { pipe, path })
    }

    pub async fn accept(&mut self) -> std::io::Result<Client> {
        self.pipe.connect().await?;
        let new_pipe = ServerOptions::new().create(&self.path)?;
        let client_pipe = Pipe::Server(std::mem::replace(&mut self.pipe, new_pipe));
        client_pipe.send(INIT_SERVER_CONNECT).await?;
        Ok(Client::new(client_pipe))
    }
}

#[derive(Debug)]
enum Pipe {
    Client(NamedPipeClient),
    Server(NamedPipeServer),
}

impl Pipe {
    async fn readable(&self) -> std::io::Result<()> {
        match self {
            Pipe::Client(v) => v.readable().await,
            Pipe::Server(v) => v.readable().await,
        }
    }

    async fn writable(&self) -> std::io::Result<()> {
        match self {
            Pipe::Client(v) => v.writable().await,
            Pipe::Server(v) => v.writable().await,
        }
    }

    fn try_recv(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Pipe::Client(v) => v.try_read(buf),
            Pipe::Server(v) => v.try_read(buf),
        }
    }

    fn try_send(&self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Pipe::Client(v) => v.try_write(buf),
            Pipe::Server(v) => v.try_write(buf),
        }
    }

    async fn recv(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            self.readable().await?;
            match self.try_recv(buf) {
                Ok(v) => return Ok(v),
                Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => continue,
                    _ => return Err(e),
                },
            }
        }
    }

    async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            self.writable().await?;
            match self.try_send(buf) {
                Ok(v) => return Ok(v),
                Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => continue,
                    _ => return Err(e),
                },
            }
        }
    }

    fn disconnect(&self) -> std::io::Result<()> {
        match self {
            Pipe::Client(_) => Ok(()),
            Pipe::Server(v) => v.disconnect(),
        }
    }
}

#[derive(Debug)]
pub struct ClientInner {
    pipe: Pipe,
    failures: AtomicUsize,
}

#[derive(Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
    handle: JoinHandle<()>,
}

impl Client {
    pub async fn open(name: &str) -> std::io::Result<Self> {
        let server_path = Path::new("\\\\.\\pipe\\").join(name);
        let pipe = Pipe::Client(
            ClientOptions::new()
                .pipe_mode(PipeMode::Message)
                .read(true)
                .write(true)
                .open(&server_path)?,
        );
        let mut buf = [0];
        let size = pipe.recv(&mut buf).await?;
        if size != 1 || buf != INIT_SERVER_CONNECT {
            return Err(std::io::Error::other("server rejected our connection"));
        }
        Ok(Self::new(pipe))
    }

    fn new(pipe: Pipe) -> Self {
        let inner = Arc::new(ClientInner {
            pipe,
            failures: AtomicUsize::new(0),
        });
        let fuck = inner.clone();
        let handle = tokio::spawn(async move {
            loop {
                if fuck.failures.load(Ordering::SeqCst) >= MAX_FAILURES {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if let Err(_) = fuck.pipe.send(ACK).await {
                    break;
                }
            }
        });
        Self { inner, handle }
    }

    fn check_dead(&self) -> std::io::Result<()> {
        if self.inner.failures.load(Ordering::SeqCst) >= MAX_FAILURES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "lost rx link",
            ));
        }
        Ok(())
    }

    fn set_dead(&self) {
        self.inner.failures.store(MAX_FAILURES, Ordering::SeqCst);
    }

    pub async fn send(&self, msg: &Message) -> std::io::Result<usize> {
        self.check_dead()?;
        let len = self.inner.pipe.send(&msg.buffer[..msg.len + 1]).await?;
        if len == 0 {
            self.set_dead();
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ));
        }
        Ok(len - 1)
    }

    pub async fn recv(&self, msg: &mut Message) -> std::io::Result<()> {
        self.check_dead()?;
        msg.set_size(msg.max_size() + 1);
        loop {
            let res = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                self.inner.pipe.recv(msg).await
            })
            .await;
            match res {
                Err(_) => {
                    let failures = self.inner.failures.fetch_add(1, Ordering::SeqCst);
                    if failures >= MAX_FAILURES {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "lost rx link",
                        ));
                    }
                }
                Ok(res) => {
                    self.inner.failures.store(0, Ordering::SeqCst);
                    let len = res?;
                    if len == 1 && msg[0] == END[0] {
                        // We've received a nominal close.
                        self.set_dead();
                        msg.set_size(0);
                        return Ok(());
                    } else if len > 0 && msg[len - 1] != ACK[0] {
                        msg.set_size(len - 1); // Remove trailing byte
                        return Ok(());
                    } else if len == 0 {
                        self.set_dead();
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "unexpected EOF",
                        ));
                    }
                }
            }
        }
    }

    pub async fn close(mut self) -> std::io::Result<()> {
        let res2 = self.inner.pipe.send(END).await;
        self.set_dead();
        let res = self.inner.pipe.disconnect();
        // one more hack because of rust garbage rules.
        let handle = std::mem::replace(&mut self.handle, tokio::spawn(async {}));
        handle.await?;
        res?;
        res2.map(|_| ())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // When dropping set the client to be dead anyway.
        self.set_dead();
        let _ = self.inner.pipe.disconnect();
    }
}
