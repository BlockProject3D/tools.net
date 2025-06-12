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

//! IPC utilities module.

use std::io::Write;
use std::ops::{Deref, DerefMut};

/// Represents an IPC message.
pub struct Message {
    pub(super) buffer: Vec<u8>,
    pub(super) len: usize
}

impl Message {
    /// Creates a new [Message](Message) for use with IPC routines.
    ///
    /// # Arguments
    ///
    /// * `max_size`: the maximum of a message to be sent or received by subsequent IPC routines.
    ///
    /// returns: Message
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: vec![0; max_size + 1],
            len: max_size
        }
    }

    /// Returns the maximum size of a message to be sent or received by subsequent IPC routines
    /// when using this [Message](Message) instance.
    pub fn max_size(&self) -> usize {
        self.buffer.len() - 1
    }

    /// Sets the current size of this [Message](Message) instance. This automatically zeroes out
    /// all unused slots.
    ///
    /// # Arguments
    ///
    /// * `len`: the new message size.
    ///
    /// returns: ()
    ///
    /// # Panics
    ///
    /// This function will panic if the given `len` is greater than the maximum size of this
    /// [Message](Message) instance.
    pub fn set_size(&mut self, len: usize) {
        assert!(len <= self.buffer.len());
        self.len = len;
        self.buffer[len..].fill(0);
    }
}

impl Deref for Message {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer[..self.len]
    }
}

impl DerefMut for Message {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer[..self.len]
    }
}

impl Write for Message {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = std::cmp::min(buf.len(), self.buffer.len() - self.len);
        self.buffer[self.len..self.len + len].copy_from_slice(&buf[..len]);
        self.len += len;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::ipc::util::Message;
    use std::io::Write;

    #[test]
    fn basic() {
        let mut msg = Message::new(1024);
        msg.set_size(0);
        write!(&mut msg, "Hello").unwrap();
        write!(&mut msg, ", World").unwrap();
        assert_eq!(&*msg, b"Hello, World");
    }
}
