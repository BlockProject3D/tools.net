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

use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::SeqCst;
use tokio::runtime::Runtime;
use bp3d_net::util::barrier::broadcast::Sender;
use crate::types::Configuration;

pub struct Core {
    pub runtime: Runtime,
    pub configuration: Configuration,
    pub kill: AtomicBool,
    pub client_count: AtomicUsize,
    pub barrier: Sender<()>
}

#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct CorePtr(*const Core);

unsafe impl Send for CorePtr {}

impl CorePtr {
    pub fn runtime(&self) -> &Runtime {
        &unsafe { &*self.0 }.runtime
    }

    pub fn configuration(&self) -> &Configuration {
        &unsafe { &*self.0 }.configuration
    }

    pub fn is_shutdown_requested(&self) -> bool {
        unsafe { &*self.0 }.kill.load(SeqCst)
    }

    pub fn request_shutdown(&self) {
        unsafe { &*self.0 }.kill.store(true, SeqCst)
    }

    pub fn is_valid(&self) -> bool {
        !self.0.is_null() && !self.is_shutdown_requested()
    }

    pub fn clients(&self) -> usize {
        unsafe { &*self.0 }.client_count.load(SeqCst)
    }

    pub fn client_connect(&self) {
        unsafe { &*self.0 }.client_count.store(self.clients() + 1, SeqCst)
    }

    pub fn client_disconnect(&self) {
        unsafe { &*self.0 }.client_count.store(self.clients() - 1, SeqCst)
    }

    pub fn barrier(&self) -> &Sender<()> {
        &unsafe { &*self.0 }.barrier
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn bp3d_net_ipc_create(configuration: *const Configuration) -> CorePtr {
    match Runtime::new() {
        Ok(runtime) => {
            CorePtr(Box::leak(Box::new(Core {
                configuration: unsafe { *configuration },
                runtime,
                kill: AtomicBool::new(false),
                client_count: AtomicUsize::new(0),
                //TODO: Maybe allow this to be configurable
                barrier: bp3d_net::util::barrier::broadcast::barrier(128)
            })))
        },
        Err(_) => {
            CorePtr(std::ptr::null_mut())
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn bp3d_net_ipc_destroy(core: CorePtr) {
    if !core.0.is_null() {
        unsafe { drop(Box::from_raw(core.0 as *mut Core)) };
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn bp3d_net_ipc_request_shutdown(core: CorePtr) {
    assert!(core.is_valid());
    core.request_shutdown();
}

#[unsafe(no_mangle)]
pub extern "C" fn bp3d_net_ipc_get_clients(core: CorePtr) -> usize {
    assert!(core.is_valid());
    core.clients()
}

#[unsafe(no_mangle)]
pub extern "C" fn bp3d_net_ipc_shutdown(core: CorePtr) {
    assert!(core.is_valid());
    core.request_shutdown();
    core.runtime().block_on(async {
        let _ = core.barrier().send(()).await;
    });
    while core.clients() > 0 {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
