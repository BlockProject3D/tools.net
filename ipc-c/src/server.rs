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

use std::ffi::{c_char, CStr, CString};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use bp3d_net::ipc::Server;
use bp3d_net::ipc::util::Message;
use crate::core::CorePtr;
use crate::types::ClientWrapper;

fn client_loop(wrapper: Arc<ClientWrapper>, core: CorePtr) -> JoinHandle<std::io::Result<()>> {
    let mut barrier = core.barrier().subscribe();
    tokio::spawn(async move {
        let mut msg = Message::new(core.configuration().packet_size);
        loop {
            tokio::select! {
                res = wrapper.client.recv(&mut msg) => {
                    res?;
                    if msg.is_empty() {
                        // The client has disconnected normally.
                        break;
                    }
                    core.configuration().recv_callback.call(Arc::as_ptr(&wrapper), msg.as_ptr(), msg.len());
                },
                _ = barrier.recv() => break
            }
        }
        Ok(())
    })
}

async fn client_loop_detached(handle: JoinHandle<std::io::Result<()>>, wrapper: Arc<ClientWrapper>, core: CorePtr) -> std::io::Result<()> {
    match handle.await? {
        Ok(()) => core.configuration().disconnect_callback.call(Arc::as_ptr(&wrapper)),
        Err(err) => {
            let msg = CString::new(err.to_string())?;
            core.configuration().error_callback.call(Arc::as_ptr(&wrapper), msg.as_ptr());
        }
    }
    let ptr = Arc::try_unwrap(wrapper).map_err(|_| ()).expect("Did client loop not terminate? This is a bug!");
    ptr.client.close().await?;
    Ok(())
}

async fn server_main(core: CorePtr, name: String) -> std::io::Result<()> {
    let mut server = Server::create(&name).await?;
    let mut barrier = core.barrier().subscribe();
    loop {
        let client = tokio::select! {
            res = server.accept() => res?,
            _ = barrier.recv() => break
        };
        let mut msg = Message::new(core.configuration().packet_size);
        msg.set_size(0);
        let wrapper = Arc::new(ClientWrapper {
            client,
            send_msg: Mutex::new(msg)
        });
        core.client_connect();
        let handle = client_loop(wrapper.clone(), core);
        tokio::spawn(async move {
            if let Err(err) = client_loop_detached(handle, wrapper, core).await {
                let msg = CString::new(err.to_string()).unwrap();
                core.configuration().error_callback.call(std::ptr::null(), msg.as_ptr());
            }
            core.client_disconnect();
        });
        if core.is_shutdown_requested() {
            break;
        }
    }
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn bp3d_net_ipc_listen(core: CorePtr, name: *const c_char) {
    assert!(core.is_valid());
    let _guard = core.runtime().enter();
    let name = unsafe { CStr::from_ptr(name) };
    let name: String = name.to_string_lossy().into();
    tokio::spawn(async move {
        if let Err(err) = server_main(core, name).await {
            let msg = CString::new(err.to_string()).unwrap();
            core.configuration().error_callback.call(std::ptr::null(), msg.as_ptr());
        }
    });
}
