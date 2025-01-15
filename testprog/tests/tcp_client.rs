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

use bp3d_net::tcp::client;
use bp3d_net::tcp::server;
use testprog::client::EchoClientFactory;
use testprog::server::EchoServerFactory;

#[tokio::test]
async fn basic() {
    let server = server::Builder::new(EchoServerFactory)
        .max_clients(5)
        .bind_local_port(4242)
        .await
        .unwrap();
    let mut client1 = client::Builder::new(EchoClientFactory)
        .connect("127.0.0.1:4242")
        .await
        .unwrap();
    let mut client2 = client::Builder::new(EchoClientFactory)
        .connect("127.0.0.1:4242")
        .await
        .unwrap();
    {
        client1
            .client()
            .request_async(String::from("hello world"))
            .await
            .unwrap();
        assert_eq!(
            client1.get_reply_async().await.unwrap().as_bytes(),
            b"hello world\n"
        );
        assert_eq!(
            client2.get_reply_async().await.unwrap().as_bytes(),
            b"hello world\n"
        );
    }
    {
        client2
            .client()
            .request_async(String::from("hello world"))
            .await
            .unwrap();
        assert_eq!(
            client1.get_reply_async().await.unwrap().as_bytes(),
            b"hello world\n"
        );
        assert_eq!(
            client2.get_reply_async().await.unwrap().as_bytes(),
            b"hello world\n"
        );
    }
    client1
        .client()
        .request_async(String::from("exit"))
        .await
        .unwrap();
    server.join().await.unwrap();
    client1.join().await.unwrap();
    client2.join().await.unwrap();
}
