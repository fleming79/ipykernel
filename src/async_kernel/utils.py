# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import contextlib
import errno
import sys
import threading
import time
from pathlib import Path
from typing import Literal

import anyio
import anyio.to_thread
from zmq import Socket, SocketType, ZMQError

from async_kernel.typing import ExecuteContent, ExecuteJobInfo, ExecuteMode, null

__all__ = ["bind_socket", "do_not_debug_this_thread", "mark_thread_pydev_do_not_trace", "wait_thread_event"]

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules


def bind_socket(
    socket: Socket, transport: Literal["tcp", "ipc"], ip: str, port: int = 0, max_attempts: int | null = null
) -> int:
    """Bind the socket to a port using the settings.

    max_attempts: The maximum number of attempts to bind the socket. If un-specified,
    defaults to 100 if port missing, else 2 attempts.
    """

    def _try_bind_socket(port: int):
        if transport == "tcp":
            if not port:
                port = socket.bind_to_random_port(f"tcp://{ip}")
            else:
                socket.bind(f"tcp://{ip}:{port}")
        elif transport == "ipc":
            if not port:
                port = 1
                while True:
                    port = port + 1
                    path = f"{ip}-{port}"
                    if not Path(path).exists():
                        break
            else:
                path = f"{ip}-{port}"
            socket.bind(f"ipc://{path}")
        return port

    if socket.TYPE == SocketType.ROUTER:
        # ref: https://github.com/ipython/ipykernel/issues/270
        socket.router_handover = 1
    try:
        win_in_use = errno.WSAEADDRINUSE  # type: ignore[attr-defined]
    except AttributeError:
        win_in_use = None
    # Try up to 100 times to bind a port when in conflict to avoid
    # infinite attempts in bad setups
    if max_attempts is null:
        max_attempts = 2 if port else 100
    e = None
    for _ in range(max_attempts):
        try:
            return _try_bind_socket(port)
        except ZMQError as e_:
            # Raise if we have any error not related to socket binding
            if e_.errno in {errno.EADDRINUSE, win_in_use}:
                e = e_
                break
            if port:
                time.sleep(1)
    msg = f"Failed to bind {socket} for {transport=}" + (f" to {port=}!" if port else "!")
    raise RuntimeError(msg) from e


def get_execute_info(content: ExecuteContent) -> ExecuteJobInfo:
    """Extract ExecuteJobInfo from the content.

    If the top line of the code starts with '#@'; the execute mode and
    namespace will be extacted from that line.

    Whitespace is stripped from


    code:
    ``` python
    # @<execute_mode>, namespace=<namespace>
    ```
    """
    mode = ExecuteMode.task if content.get("silent", True) else ExecuteMode.queue
    namespace = ""
    if (code := content["code"].strip()).startswith("#@") and (header := code.split("\n", maxsplit=1)[0]):
        match header.split(",")[0].strip().removeprefix("#@").lower():
            case "task":
                mode = ExecuteMode.task
            case "thread":
                mode = ExecuteMode.thread
        if len(s := header.split("namespace=", maxsplit=1)) == 2:
            namespace = s[1].strip().strip("'\"")
            assert "," not in namespace, "Reserved symbol detected!"
    return ExecuteJobInfo(execute_mode=mode, namespace=namespace)


def mark_thread_pydev_do_not_trace(thread: threading.Thread, name="", *, remove=False):
    """Modifies the given thread's attributes to hide or unhide it from the debugger (e.g., debugpy)."""
    thread.pydev_do_not_trace = not remove  # type: ignore[attr-defined]
    if name:
        thread.name = name


@contextlib.contextmanager
def do_not_debug_this_thread(name=""):
    "A context to mark the thread for debugpy to not debug."
    if not LAUNCHED_BY_DEBUGPY:
        mark_thread_pydev_do_not_trace(threading.current_thread(), name)
    try:
        yield
    finally:
        if not LAUNCHED_BY_DEBUGPY:
            mark_thread_pydev_do_not_trace(threading.current_thread(), remove=True)


async def wait_thread_event(event: threading.Event):
    """Wait for the threading event in a separate thread.

    The event will be set event if the coroutine is cancelled to ensure the thread is cleared.
    """

    def _in_thread_call():
        with do_not_debug_this_thread():
            event.wait()

    try:
        await anyio.to_thread.run_sync(_in_thread_call)
    finally:
        event.set()
