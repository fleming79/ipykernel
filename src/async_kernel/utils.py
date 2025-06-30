# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import errno
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import anyio
import anyio.to_thread
import sniffio
import zmq

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType

    from anyio.abc import TaskGroup, TaskStatus

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules


def start_anyio_thread(
    func: Callable[[TaskStatus], CoroutineType],
    stop_event: threading.Event,
    tg: TaskGroup,
    *,
    backend: Literal["anyio", "trio", ""] = "",
    name="",
    pydev_do_not_trace=not LAUNCHED_BY_DEBUGPY,
    is_pydev_daemon_thread=not LAUNCHED_BY_DEBUGPY,
):
    """Run a coroutine function in a separate thread (and event loop) and manage its lifecycle using AnyIO.

    This function takes an asynchronous function, a stop event, and a task group,
    and starts the function in a separate thread. It ensures that the function
    is properly started and can be stopped gracefully.

    This coroutine returns once `task_status.started()`is called inside `func`
    running until the `stop_event` is set.

    Args:
        func: The asynchronous function to run in a separate thread. It should
            accept a TaskStatus object as an argument and return a coroutine.
        stop_event: A threading.Event that signals when the function should stop.
        tg: The AnyIO TaskGroup to use for managing the function's task.
        task_status: An AnyIO TaskStatus object to signal when the function has started.
    """

    backend = backend or sniffio.current_async_library()  # type: ignore[no-any-return]
    ready_event = threading.Event()

    def run_func():
        thread = threading.current_thread()
        if name:
            thread.name = name
        thread.pydev_do_not_trace = pydev_do_not_trace  # type: ignore[attr-defined]
        thread.is_pydev_daemon_thread = is_pydev_daemon_thread  # type: ignore[attr-defined]

        async def run_until_stop_event():
            async with anyio.create_task_group() as tg:
                await tg.start(func)
                ready_event.set()

                def _wait_stop():
                    thread = threading.current_thread()
                    thread.pydev_do_not_trace = True  # type: ignore[attr-defined]
                    thread.is_pydev_daemon_thread = True  # type: ignore[attr-defined]
                    stop_event.wait()

                await anyio.to_thread.run_sync(_wait_stop)
                tg.cancel_scope.cancel()

        anyio.run(run_until_stop_event, backend=backend)

    tg.start_soon(anyio.to_thread.run_sync, run_func)
    return anyio.to_thread.run_sync(ready_event.wait)


def bind_socket(socket: zmq.Socket, transport: Literal["tcp", "ipc"], ip: str, port: int = 0, max_attempts=100) -> int:
    def _try_bind_socket(port: int):
        if transport == "tcp":
            if port <= 0:
                port = socket.bind_to_random_port(f"tcp://{ip}")
            else:
                socket.bind(f"tcp://{ip}:{port}")
        elif transport == "ipc":
            if port <= 0:
                port = 1
                while True:
                    port = port + 1
                    path = f"{ip}-{port}"
                    if Path(path).exists():
                        break
            else:
                path = f"{ip}-{port}"
            socket.bind(f"ipc://{path}")
        return port

    try:
        win_in_use = errno.WSAEADDRINUSE  # type: ignore[attr-defined]
    except AttributeError:
        win_in_use = None
    # Try up to 100 times to bind a port when in conflict to avoid
    # infinite attempts in bad setups
    max_attempts = 1 if port else max_attempts
    for attempt in range(max_attempts):
        try:
            return _try_bind_socket(port)
        except zmq.ZMQError as e:
            # Raise if we have any error not related to socket binding
            if e.errno != errno.EADDRINUSE and e.errno != win_in_use:
                raise
            if attempt == max_attempts - 1:
                raise
    msg = f"Failed to bind a {socket}:{port}"
    raise RuntimeError(msg)
