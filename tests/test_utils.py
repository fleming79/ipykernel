# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from typing import Literal

import pytest
import zmq

from async_kernel.utils import bind_socket


@pytest.fixture(scope="module", params=["tcp", "ipc"])
def transport(request):
    return request.param


def test_bind_socket(transport: Literal["tcp", "ipc"], tmp_path):
    if transport == "ipc" and not zmq.has("ipc"):
        pytest.skip("transport='ipc' not supported.")

    ctx = zmq.Context()
    ip = tmp_path / "mypath" if transport == "ipc" else "0.0.0.0"
    with ctx:
        with ctx.socket(zmq.SocketType.ROUTER) as socket:
            port = bind_socket(socket, transport, ip)
        with ctx.socket(zmq.SocketType.ROUTER) as socket:
            assert bind_socket(socket, transport, ip, port) == port
            if transport == "tcp":
                with pytest.raises(RuntimeError):
                    bind_socket(socket, transport, ip, "invalid port")  # type: ignore[call-arg]
