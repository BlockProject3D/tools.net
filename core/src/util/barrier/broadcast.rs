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

//! A multi-client barrier synchronization primitive.

use crate::util::barrier::{Error, Msg};
use bp3d_debug::trace;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use tokio::sync::{broadcast, Semaphore};

/// Represents a multi-client barrier lock.
pub struct Lock<T> {
    msg: Msg<T>,
}

/// Represents a multi-client sender.
pub struct Sender<T> {
    inner: broadcast::Sender<Msg<T>>,
    closed: AtomicBool,
}

/// Represents a multi-client receiver.
pub struct Receiver<T> {
    inner: broadcast::Receiver<Msg<T>>,
}

impl<T> Deref for Lock<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.msg.inner
    }
}

impl<T> DerefMut for Lock<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.msg.inner
    }
}

impl<T> Drop for Lock<T> {
    fn drop(&mut self) {
        unsafe {
            (*self.msg.synchro).add_permits(1);
        }
    }
}

impl<T: Debug> Debug for Lock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.msg.inner.fmt(f)
    }
}

impl<T> Sender<T> {
    /// Close the barrier.
    pub fn close(&self) {
        self.closed.store(true, Relaxed);
    }

    /// Send a new message onto the channel.
    ///
    /// # Panics
    ///
    /// This function panics if the semaphore used for barrier synchronization fails.
    pub async fn send(&self, msg: T) -> Result<(), Error> {
        if self.closed.load(Relaxed) {
            return Err(Error::Closed);
        }
        let semaphore = Semaphore::new(0);
        let msg = Msg {
            synchro: &semaphore,
            inner: msg,
        };
        let count = self.inner.send(msg).map_err(|_| Error::BrokenPipe)?;
        trace!("Waiting for {} client(s) to acknowledge", count);
        let _ = semaphore.acquire_many(count as _).await.unwrap();
        Ok(())
    }

    /// Creates and subscribes a new receiver to this sender.
    pub fn subscribe(&self) -> Receiver<T> {
        Receiver {
            inner: self.inner.subscribe(),
        }
    }
}

impl<T: Clone> Receiver<T> {
    /// Attempts to receive a message from the channel.
    ///
    /// # Errors
    ///
    /// This returns an [Error] if the channel was prematurely closed.
    pub async fn recv(&mut self) -> Result<Lock<T>, Error> {
        self.inner
            .recv()
            .await
            .map_err(|_| Error::BrokenPipe)
            .map(|msg| Lock { msg })
    }
}

/// Creates a new multi-client synchronization barrier.
///
/// # Arguments
///
/// * `len`: the maximum number of synchronization allowed at a time.
///
/// returns: Sender<T>
pub fn barrier<T: Clone>(len: usize) -> Sender<T> {
    let (sender, _) = broadcast::channel(len);
    Sender {
        inner: sender,
        closed: AtomicBool::new(false),
    }
}
