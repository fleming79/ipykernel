# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from typing import Literal

import pytest
import zmq

from async_kernel.typing import ExecuteContent, ExecuteJobInfo, ExecuteMode
from async_kernel.utils import bind_socket, get_execute_info


@pytest.fixture(scope="module", params=["tcp", "ipc"])
def transport(request):
    return request.param


def test_bind_socket(transport: Literal["tcp", "ipc"], tmp_path):
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


@pytest.mark.parametrize(
    ("code", "silent", "expected"),
    [
        ("#@task", False, ExecuteJobInfo(execute_mode=ExecuteMode.task, namespace_id="")),
        ("print(1)", False, ExecuteJobInfo(execute_mode=ExecuteMode.queue, namespace_id="")),
        ("", True, ExecuteJobInfo(execute_mode=ExecuteMode.task, namespace_id="")),
        (
            "#@thread, namespace_id= My namespace_id \nprint('hello')",
            False,
            ExecuteJobInfo(execute_mode=ExecuteMode.thread, namespace_id="My namespace_id"),
        ),
        (
            "#@namespace_id=1 @!%n🌋 \nprint(None)",
            False,
            ExecuteJobInfo(execute_mode=ExecuteMode.queue, namespace_id="1 @!%n🌋"),
        ),
    ],
)
def test_get_execute_info(code: str, silent: bool, expected: dict):
    content = ExecuteContent(
        code=code,
        silent=silent,
        store_history=True,
        user_expressions={},
        allow_stdin=False,
        stop_on_error=True,
    )
    execute_info = get_execute_info(content)
    assert execute_info == expected
