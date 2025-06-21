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
use tempfile::TempDir;
use tokio::net::UnixDatagram;
use tokio::task::JoinHandle;

const INIT_CLIENT_CONNECT: &[u8] = &[0xAB];
const INIT_SERVER_CONNECT: &[u8] = &[0xCD];
const ACK: &[u8] = &[0xFF];
const END: &[u8] = &[0xFE];

const MAX_FAILURES: usize = 2;

#[derive(Debug)]
pub struct Server {
    socket: UnixDatagram,
    dir: TempDir,
    num_clients: AtomicUsize,
    path: PathBuf,
}

impl Server {
    pub async fn create(name: &str) -> std::io::Result<Self> {
        let tmp_path = std::env::var_os("TMPDIR").map(PathBuf::from).unwrap_or(PathBuf::from("/tmp"));
        let path = tmp_path.join(name);
        let _ = tokio::fs::remove_file(&path).await;
        let socket = UnixDatagram::bind(&path)?;
        let dir = tempfile::tempdir()?;
        Ok(Self {
            socket,
            dir,
            num_clients: AtomicUsize::new(0),
            path,
        })
    }

    pub async fn accept(&self) -> std::io::Result<Client> {
        let mut buf = [0; 1];
        let (len, tx) = self.socket.recv_from(&mut buf).await?;
        let tx = tx
            .as_pathname()
            .ok_or(std::io::Error::other("unable to establish tx link"))?
            .into();
        if len != 1 || buf != INIT_CLIENT_CONNECT {
            self.socket.send_to(END, &tx).await?;
            return Err(std::io::Error::other("rejected invalid client"));
        }
        let cur = self.num_clients.fetch_add(1, Ordering::Relaxed);
        let name = format!("client_{cur}");
        let rx_path = self.dir.path().join(name);
        let rx = UnixDatagram::bind(rx_path)?;
        rx.send_to(INIT_SERVER_CONNECT, &tx).await?;
        Ok(Client::new(rx, tx, None))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug)]
pub struct ClientInner {
    rx: UnixDatagram,
    tx: PathBuf,
    // This is needed because this must not be dropped until ClientInner itself is dropped.
    #[allow(unused)]
    dir: Option<TempDir>,
    failures: AtomicUsize,
}

#[derive(Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
    handle: JoinHandle<()>,
}

impl Client {
    pub async fn open(name: &str) -> std::io::Result<Self> {
        let dir = tempfile::tempdir()?;
        let tmp_path = std::env::var_os("TMPDIR").map(PathBuf::from).unwrap_or(PathBuf::from("/tmp"));
        let server_path = tmp_path.join(name);
        let client_path = dir.path().join("client");
        let rx = UnixDatagram::bind(client_path)?;
        rx.send_to(INIT_CLIENT_CONNECT, server_path).await?;
        let mut buf = [0];
        let (size, tx) = rx.recv_from(&mut buf).await?;
        if size != 1 || buf != INIT_SERVER_CONNECT {
            return Err(std::io::Error::other("server rejected our connection"));
        }
        let tx = tx
            .as_pathname()
            .ok_or(std::io::Error::other("unable to establish tx link"))?
            .into();
        Ok(Self::new(rx, tx, Some(dir)))
    }

    fn new(rx: UnixDatagram, tx: PathBuf, dir: Option<TempDir>) -> Self {
        let inner = Arc::new(ClientInner {
            rx,
            tx,
            dir,
            failures: AtomicUsize::new(0),
        });
        let fuck = inner.clone();
        let handle = tokio::spawn(async move {
            loop {
                if fuck.failures.load(Ordering::SeqCst) >= MAX_FAILURES {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if fuck.rx.send_to(ACK, &fuck.tx).await.is_err() {
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
        let len = self
            .inner
            .rx
            .send_to(&msg.buffer[..msg.len + 1], &self.inner.tx)
            .await?;
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
                self.inner.rx.recv(msg).await
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
        let res = self.inner.rx.send_to(END, &self.inner.tx).await;
        self.set_dead();
        // one more hack because of rust garbage rules.
        let handle = std::mem::replace(&mut self.handle, tokio::spawn(async {}));
        handle.await?;
        res.map(|_| ())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // When dropping set the client to be dead anyway.
        self.set_dead();
    }
}
