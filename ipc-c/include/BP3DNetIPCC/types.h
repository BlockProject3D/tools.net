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

#ifndef BP3D_NET_IPC_TYPES
#define BP3D_NET_IPC_TYPES

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

typedef const void* bp3d_net_ipc_client_t;

typedef void(*bp3d_net_ipc_connect_fn_t)(void* udata, bp3d_net_ipc_client_t client);

typedef void(*bp3d_net_ipc_disconnect_fn_t)(void* udata, bp3d_net_ipc_client_t client);

typedef void(*bp3d_net_ipc_recv_fn_t)(void* udata, bp3d_net_ipc_client_t client, const uint8_t* data, size_t size);

typedef void(*bp3d_net_ipc_error_fn_t)(void* udata, bp3d_net_ipc_client_t client, bool is_dead, const char* msg);

#define BP3D_NET_IPC_CALLBACK(name, type) \
    typedef struct { \
        void* udata; \
        type func; \
    } bp3d_net_ipc_##name##_callback_t;

BP3D_NET_IPC_CALLBACK(connect, bp3d_net_ipc_connect_fn_t);
BP3D_NET_IPC_CALLBACK(disconnect, bp3d_net_ipc_disconnect_fn_t);
BP3D_NET_IPC_CALLBACK(recv, bp3d_net_ipc_recv_fn_t);
BP3D_NET_IPC_CALLBACK(error, bp3d_net_ipc_error_fn_t);

typedef struct {
    bp3d_net_ipc_connect_callback_t connect_callback;
    bp3d_net_ipc_disconnect_callback_t disconnect_callback;
    bp3d_net_ipc_recv_callback_t recv_callback;
    bp3d_net_ipc_error_callback_t error_callback;
    size_t packet_size;
} bp3d_net_ipc_configuration_t;

#endif
