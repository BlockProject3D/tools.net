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

//! A multi-client TCP server implementation designed for long-running connections.

mod server;
mod client;

pub use server::{Builder, Server, ServerApp, Handler, Factory};
pub use client::Handler as ClientHandler;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use bp3d_debug::debug;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use crate::tcp::server::{Builder, ClientHandler, Factory, Handler, Server};
    use crate::tcp::util::Network;

    struct EchoServer {
        server: Arc<Server<()>>,
    }

    impl Handler for EchoServer {
        type ClientHandler = EchoServer;
        type Event = ();

        async fn connect(&mut self, _: &mut Network) -> std::io::Result<Self::ClientHandler> {
            Ok(EchoServer { server: self.server.clone() })
        }
    }

    impl ClientHandler for EchoServer {
        async fn recv(&mut self, net: &mut Network) -> std::io::Result<()> {
            let mut s = String::new();
            net.read_line(&mut s).await?;
            debug!("Received: {:?}", s);
            if s == "exit\n" {
                self.server.exit();
            }
            let motherfuckingrust = self.server.clone();
            tokio::spawn(async move {
                motherfuckingrust.broadcast(s.as_bytes()).await;
            });
            Ok(())
        }
    }

    struct EchoServerFactory;

    impl Factory for EchoServerFactory {
        type Handler = EchoServer;

        fn start(self, server: &Arc<Server<()>>) -> Self::Handler {
            EchoServer { server: server.clone() }
        }
    }

    #[tokio::test]
    async fn basic() {
        let server = Builder::new(EchoServerFactory).max_clients(5).bind_local_port(4242).await.unwrap();
        let mut client1 = TcpStream::connect("127.0.0.1:4242").await.unwrap();
        let mut client2 = TcpStream::connect("127.0.0.1:4242").await.unwrap();
        let mut buf = [0; 12];
        {
            client1.write_all(b"hello world\n").await.unwrap();
            client1.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello world\n");
            buf.fill(0x0);
            client2.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello world\n");
            buf.fill(0x0);
        }
        {
            client2.write_all(b"hello world\n").await.unwrap();
            client1.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello world\n");
            buf.fill(0x0);
            client2.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello world\n");
            buf.fill(0x0);
        }
        client1.write_all(b"exit\n").await.unwrap();
        server.join().await.unwrap();
        assert!(client1.read_exact(&mut buf).await.is_err());
        assert!(client2.read_exact(&mut buf).await.is_err());
    }
}
