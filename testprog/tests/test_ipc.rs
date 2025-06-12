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

use bp3d_net::ipc::util::Message;
use bp3d_net::ipc::{Client, Server};

#[tokio::test]
async fn improper_terminate() {
    let mut server = Server::create("_my_test_ipc_server_").await.unwrap();
    let handle = tokio::spawn(async move {
        let client = server.accept().await.unwrap();
        let mut msg = Message::new(256);
        let mut flag = false;
        loop {
            let time = std::time::Instant::now();
            if let Err(_) = client.recv(&mut msg).await {
                break;
            }
            if msg.len() == 0 {
                println!("Nominal client disconnect");
                flag = true;
                break;
            }
            if let Err(_) = client.send(&msg).await {
                break;
            }
            let duration = time.elapsed();
            println!("server cycle: {}", duration.as_secs_f64());
        }
        assert!(!flag);
    });
    let time = std::time::Instant::now();
    {
        let client = Client::open("_my_test_ipc_server_").await.unwrap();
        let time = std::time::Instant::now();
        {
            let mut msg = Message::new(256);
            msg.set_size(11);
            msg.copy_from_slice("hello world".as_bytes());
            client.send(&msg).await.unwrap();
            client.recv(&mut msg).await.unwrap();
            assert_eq!(11, msg.len());
            assert_eq!(b"hello world", &*msg);
        }
        let duration = time.elapsed();
        println!("client cycle: {}", duration.as_secs_f64());
    }
    let duration = time.elapsed();
    println!(
        "full client connect-send-recv cycle: {}",
        duration.as_secs_f64()
    );
    handle.await.unwrap();
}

#[tokio::test]
async fn basic() {
    let mut server = Server::create("_my_test_ipc_server_1_").await.unwrap();
    let handle = tokio::spawn(async move {
        let client = server.accept().await.unwrap();
        let mut msg = Message::new(256);
        let mut flag = false;
        loop {
            if let Err(_) = client.recv(&mut msg).await {
                break;
            }
            if msg.len() == 0 {
                println!("Nominal client disconnect");
                flag = true;
                break;
            }
            if let Err(_) = client.send(&msg).await {
                break;
            }
        }
        assert!(flag);
        client.close().await.unwrap();
    });
    let client = Client::open("_my_test_ipc_server_1_").await.unwrap();
    let mut msg = Message::new(256);
    msg.set_size(11);
    msg.copy_from_slice("hello world".as_bytes());
    client.send(&msg).await.unwrap();
    client.recv(&mut msg).await.unwrap();
    assert_eq!(11, msg.len());
    assert_eq!(b"hello world", &*msg);
    client.close().await.unwrap();
    handle.await.unwrap();
}
