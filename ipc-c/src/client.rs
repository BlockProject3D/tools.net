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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use tokio::sync::Mutex;
use bp3d_net::ipc::Client;
use bp3d_net::ipc::util::Message;
use crate::core::CorePtr;
use crate::types::ClientWrapper;

async fn client_loop(wrapper: Arc<ClientWrapper>, core: CorePtr) -> std::io::Result<()> {
    core.configuration().connect_callback.call(Arc::as_ptr(&wrapper));
    let mut barrier = core.barrier().subscribe();
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
                if wrapper.eject.load(SeqCst) || core.is_shutdown_requested() {
                    break;
                }
            },
            _ = barrier.recv() => break
        }
    }
    Ok(())
}

async fn client_main(core: CorePtr, name: String) -> std::io::Result<()> {
    let client = Client::open(&name).await?;
    core.client_connect();
    let mut msg = Message::new(core.configuration().packet_size);
    msg.set_size(0);
    let wrapper = Arc::new(ClientWrapper {
        client,
        send_msg: Mutex::new(msg),
        eject: AtomicBool::new(false),
    });
    match client_loop(wrapper.clone(), core).await {
        Ok(()) => core.configuration().disconnect_callback.call(Arc::as_ptr(&wrapper)),
        Err(err) => {
            let msg = CString::new(err.to_string()).expect("invalid io error, this is a bug!");
            core.configuration().error_callback.call(Arc::as_ptr(&wrapper), true, msg.as_ptr());
        }
    }
    let ptr = Arc::try_unwrap(wrapper).map_err(|_| ()).expect("Did client loop not terminate? This is a bug!");
    ptr.client.close().await?;
    core.client_disconnect();
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn bp3d_net_ipc_connect(core: CorePtr, name: *const c_char) {
    assert!(core.is_valid());
    let _guard = core.runtime().enter();
    let name = unsafe { CStr::from_ptr(name) };
    let name: String = name.to_string_lossy().into();
    tokio::spawn(async move {
        if let Err(err) = client_main(core, name).await {
            let msg = CString::new(err.to_string()).expect("invalid io error, this is a bug!");
            core.configuration().error_callback.call(std::ptr::null(), false, msg.as_ptr());
        }
    });
}

#[repr(transparent)]
pub struct ClientPtr(*const ClientWrapper);

impl ClientPtr {
    pub fn is_ejected(&self) -> bool {
        unsafe { (*self.0).eject.load(SeqCst) }
    }

    pub fn is_valid(&self) -> bool {
        !self.0.is_null() && !self.is_ejected()
    }

    pub fn eject(&self) {
        unsafe { (*self.0).eject.store(true, SeqCst) }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn bp3d_net_ipc_client_eject(client: ClientPtr) {
    assert!(client.is_valid());
    client.eject();
}

#[unsafe(no_mangle)]
pub extern "C" fn bp3d_net_ipc_client_send(core: CorePtr, client: ClientPtr, data: *const u8, size: usize) {
    assert!(client.is_valid());
    let client = unsafe { Arc::from_raw(client.0) };
    {
        let mut dst_msg = client.send_msg.blocking_lock();
        dst_msg.set_size(size);
        let slice = unsafe { std::slice::from_raw_parts(data, size) };
        dst_msg.copy_from_slice(slice);
        let client = client.clone();
        tokio::spawn(async move {
            let send_msg = client.send_msg.lock().await;
            if let Err(err) = client.client.send(&send_msg).await {
                let msg = CString::new(err.to_string()).expect("invalid io error, this is a bug!");
                core.configuration().error_callback.call(Arc::as_ptr(&client), false, msg.as_ptr());
            }
        });
    }
    std::mem::forget(client);
}
